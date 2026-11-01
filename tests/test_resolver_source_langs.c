/* Source adapter rules, one fixture per language, through the whole pipeline on
 * the sequential and the parallel route. Forms are taken from plug, cowboy, zls,
 * mojo and sinatra. Each positive is a rule of the language; each negative is a
 * same-named declaration or an effect the rule must not bind through. */
#include "cbm.h"
#include "pipeline/pipeline.h"
#include "resolve/policy.h"
#include "resolver_quality.h"
#include "store/store.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <stdlib.h>
#include <string.h>

#define SL_CALL_AT(id, sfile, scaller, text, at, tfile, tsuffix, tlabel, tline, tdecl)         \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "CALLS",                                                                   \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = tlabel,                                                            \
                   .declaration_line = tline,                                                  \
                   .declaration_text = tdecl}                                                  \
    }
#define SL_DENY_AT(id, sfile, scaller, text, at)                                               \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "CALLS", .any_target = true                                                \
    }

static bool sl_write(const char *repo, const RQ_Source *source) {
    char directory[1024];
    snprintf(directory, sizeof(directory), "%s/%s", repo, source->path);
    *strrchr(directory, '/') = '\0';
    return cbm_mkdir_p(directory, 0755) &&
           th_write_file(TH_PATH(repo, source->path), source->text) == 0;
}

/* Runs both routes; a filler per file pushes the parallel route past its minimum. */
static bool sl_check(const char *project, const char *filler_name, const char *filler_text,
                     const RQ_Source *sources, size_t source_count, const RQ_Expected *expected,
                     size_t expected_count, const RQ_Forbidden *forbidden, size_t forbidden_count) {
    bool ok = true;
    for (int driver = 0; ok && driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_source_langs");
        if (!root)
            return false;
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        ok = cbm_mkdir_p(repo, 0755);
        for (size_t i = 0; ok && i < source_count; i++)
            ok = sl_write(repo, &sources[i]);
        for (int i = 0; ok && i < 55; i++) {
            char path[64];
            snprintf(path, sizeof(path), "filler/f%02d%s", i, filler_name);
            RQ_Source filler = {.path = path, .text = filler_text};
            ok = sl_write(repo, &filler);
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, project) &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else {
            cbm_unsetenv("CBM_WORKERS");
        }
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, project, project, sources, source_count, expected, expected_count,
                      forbidden, forbidden_count, &report, stderr) &&
             ok;
        if (store)
            cbm_store_close(store);
        th_cleanup(root);
    }
    return ok;
}

/* ── Elixir ────────────────────────────────────────────────────────── */

static const char EX_CONN[] =
    "defmodule Plug.Conn do\n"
    "  alias Plug.Conn.Status\n"
    "  alias Plug.Conn.Status, as: S\n"
    "  alias Plug.{Cookies, Upload.File}\n"
    "  import Plug.Helpers, only: [assign_it: 2]\n"
    "  import Plug.Open\n"
    "\n"
    "  def local(conn), do: validate(conn, 1)\n"
    "  def piped(conn), do: conn |> validate(1)\n"
    "  def full(conn), do: Plug.Conn.Status.code(conn)\n"
    "  def aliased(conn), do: Status.code(conn)\n"
    "  def renamed(conn), do: S.reason(conn)\n"
    "  def grouped(conn), do: Cookies.fetch(conn)\n"
    "  def grouped_path(conn), do: File.path(conn)\n"
    "  def own_module(conn), do: __MODULE__.local(conn)\n"
    "  def imported_only(conn), do: assign_it(conn, 1)\n"
    "  def imported_open(conn), do: from_open(conn)\n"
    "  def recursive(0), do: 0\n"
    "  def recursive(n), do: recursive(n - 1)\n"
    "\n"
    "  def no_arity(conn), do: validate(conn)\n"
    "  def no_filter(conn), do: not_listed(conn)\n"
    "  def no_decoy(conn), do: code(conn)\n"
    "  def no_private(conn), do: Plug.Conn.Status.hidden(conn)\n"
    "  def no_variable(mod), do: mod.code(1)\n"
    "  def no_anonymous(fun), do: fun.(1)\n"
    "  def no_capture, do: Enum.map([1], &Status.code/1)\n"
    "  def no_quote(conn) do\n"
    "    quote do\n"
    "      validate(unquote(conn), 1)\n"
    "    end\n"
    "  end\n"
    "\n"
    "  defp validate(conn, _), do: conn\n"
    "end\n";
static const char EX_STATUS[] = "defmodule Plug.Conn.Status do\n"
                                "  def code(x), do: x\n"
                                "  def reason(x), do: x\n"
                                "  defp hidden(x), do: x\n"
                                "end\n";
static const char EX_COOKIES[] = "defmodule Plug.Cookies do\n"
                                 "  def fetch(x), do: x\n"
                                 "end\n";
static const char EX_FILE[] = "defmodule Plug.Upload.File do\n"
                              "  def path(x), do: x\n"
                              "end\n";
static const char EX_HELPERS[] = "defmodule Plug.Helpers do\n"
                                 "  def assign_it(conn, _), do: conn\n"
                                 "  def not_listed(conn), do: conn\n"
                                 "end\n";
static const char EX_OPEN[] = "defmodule Plug.Open do\n"
                              "  def from_open(conn), do: conn\n"
                              "end\n";
static const char EX_DECOY[] = "defmodule Decoy do\n"
                               "  def code(x), do: x\n"
                               "  def validate(x), do: x\n"
                               "end\n";
static const char EX_NEST[] = "defmodule Outer do\n"
                              "  def outer_only(a), do: a\n"
                              "  defmodule Inner do\n"
                              "    def inner_call, do: outer_only(1)\n"
                              "    def via_nested_alias, do: Inner.inner_call()\n"
                              "  end\n"
                              "end\n";
static const char EX_SHARED[] = "defmodule SharedA do\n"
                                "  def same(x), do: x\n"
                                "  def calls_same, do: same(1)\n"
                                "end\n"
                                "defmodule SharedB do\n"
                                "  def same(x), do: x\n"
                                "end\n";

#define EX_MAIN "lib/plug/conn.ex"
#define EX_CALL(id, method, text, tfile, tsuffix, tline, tdecl)                                \
    SL_CALL_AT(id, EX_MAIN, "conn." method, text, 0, tfile, tsuffix, "Function", tline, tdecl)
#define EX_DENY(id, method, text) SL_DENY_AT(id, EX_MAIN, "conn." method, text, 0)

TEST(resolver_source_elixir_modules_aliases_imports_and_arity) {
    static const RQ_Source sources[] = {
        {.path = EX_MAIN, .text = EX_CONN},
        {.path = "lib/plug/conn/status.ex", .text = EX_STATUS},
        {.path = "lib/plug/cookies.ex", .text = EX_COOKIES},
        {.path = "lib/plug/upload/file.ex", .text = EX_FILE},
        {.path = "lib/plug/helpers.ex", .text = EX_HELPERS},
        {.path = "lib/plug/open.ex", .text = EX_OPEN},
        {.path = "lib/decoy.ex", .text = EX_DECOY},
        {.path = "lib/nest.ex", .text = EX_NEST},
        {.path = "lib/shared.ex", .text = EX_SHARED},
    };
    static const RQ_Expected expected[] = {
        EX_CALL("local-name-and-arity", "local", "validate(conn, 1)", EX_MAIN, "conn.validate", 34,
                "  defp validate(conn, _), do: conn"),
        EX_CALL("pipe-supplies-first-argument", "piped", "validate(1)", EX_MAIN, "conn.validate",
                34, "  defp validate(conn, _), do: conn"),
        EX_CALL("fully-spelled-module", "full", "Plug.Conn.Status.code(conn)",
                "lib/plug/conn/status.ex", "status.code", 2, "  def code(x), do: x"),
        SL_CALL_AT("alias-last-segment", EX_MAIN, "conn.aliased", "Status.code(conn)", 1,
                   "lib/plug/conn/status.ex", "status.code", "Function", 2,
                   "  def code(x), do: x"),
        EX_CALL("alias-as", "renamed", "S.reason(conn)", "lib/plug/conn/status.ex", "status.reason",
                3, "  def reason(x), do: x"),
        EX_CALL("multi-alias", "grouped", "Cookies.fetch(conn)", "lib/plug/cookies.ex",
                "cookies.fetch", 2, "  def fetch(x), do: x"),
        EX_CALL("multi-alias-dotted-member", "grouped_path", "File.path(conn)",
                "lib/plug/upload/file.ex", "file.path", 2, "  def path(x), do: x"),
        EX_CALL("module-self-reference", "own_module", "__MODULE__.local(conn)", EX_MAIN,
                "conn.local", 8, "  def local(conn), do: validate(conn, 1)"),
        EX_CALL("import-only-list", "imported_only", "assign_it(conn, 1)", "lib/plug/helpers.ex",
                "helpers.assign_it", 2, "  def assign_it(conn, _), do: conn"),
        EX_CALL("import-whole-module", "imported_open", "from_open(conn)", "lib/plug/open.ex",
                "open.from_open", 2, "  def from_open(conn), do: conn"),
        EX_CALL("clause-calling-its-own-function", "recursive", "recursive(n - 1)", EX_MAIN,
                "conn.recursive", 19, "  def recursive(n), do: recursive(n - 1)"),
    };
    static const RQ_Forbidden forbidden[] = {
        EX_DENY("wrong-arity-is-another-function", "no_arity", "validate(conn)"),
        EX_DENY("name-outside-only-list", "no_filter", "not_listed(conn)"),
        SL_DENY_AT("same-name-in-unimported-module", EX_MAIN, "conn.no_decoy", "code(conn)", 2),
        EX_DENY("private-from-another-module", "no_private", "Plug.Conn.Status.hidden(conn)"),
        EX_DENY("variable-receiver", "no_variable", "mod.code(1)"),
        EX_DENY("anonymous-function-call", "no_anonymous", "fun.(1)"),
        SL_DENY_AT("capture-is-not-an-invocation", EX_MAIN, "conn.no_capture", "Status.code", 2),
        EX_DENY("quote-belongs-to-expansion-site", "no_quote", "validate(unquote(conn), 1)"),
        SL_DENY_AT("enclosing-module-function-is-not-visible", "lib/nest.ex", "nest.inner_call",
                   "outer_only(1)", 0),
        SL_DENY_AT("graph-node-shared-by-two-modules", "lib/shared.ex", "shared.calls_same",
                   "same(1)", 0),
    };
    ASSERT(sl_check("elixir-rules", ".ex", "defmodule Filler do\n  def f, do: 1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Erlang ────────────────────────────────────────────────────────── */

static const char ERL_REQ[] = "-module(cowboy_req).\n"
                              "-export([reply/2, set/1]).\n"
                              "\n"
                              "reply(S, R) -> do_reply(S, R).\n"
                              "set(X) -> ?MODULE:reply(X, X).\n"
                              "do_reply(0, R) -> R;\n"
                              "do_reply(S, R) -> do_reply(S - 1, R).\n"
                              "hidden(X) -> X.\n";
static const char ERL_CALLER[] = "-module(caller).\n"
                                 "-export([remote/1, imported/1]).\n"
                                 "-import(cowboy_req, [reply/2]).\n"
                                 "\n"
                                 "remote(X) -> cowboy_req:reply(X, X).\n"
                                 "imported(X) -> reply(X, X).\n"
                                 "no_export(X) -> cowboy_req:hidden(X).\n"
                                 "no_module(M, X) -> M:reply(X, X).\n"
                                 "no_function(F, X) -> cowboy_req:F(X).\n"
                                 "no_arity(X) -> reply(X).\n"
                                 "no_decoy(X) -> set(X).\n"
                                 "no_fun(F, X) -> F(X).\n"
                                 "no_absent(X) -> cowboy_req:absent(X).\n";
static const char ERL_DECOY[] = "-module(decoy).\n"
                                "-export([set/1, reply/2]).\n"
                                "\n"
                                "set(X) -> X.\n"
                                "reply(A, B) -> {A, B}.\n";

TEST(resolver_source_erlang_module_arity_export_and_import) {
    static const RQ_Source sources[] = {
        {.path = "src/cowboy_req.erl", .text = ERL_REQ},
        {.path = "src/caller.erl", .text = ERL_CALLER},
        {.path = "src/decoy.erl", .text = ERL_DECOY},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("remote-exported-function", "src/caller.erl", "caller.remote",
                   "cowboy_req:reply(X, X)", 0, "src/cowboy_req.erl", "cowboy_req.reply",
                   "Function", 4, "reply(S, R) -> do_reply(S, R)."),
        SL_CALL_AT("import-attribute", "src/caller.erl", "caller.imported", "reply(X, X)", 1,
                   "src/cowboy_req.erl", "cowboy_req.reply", "Function", 4,
                   "reply(S, R) -> do_reply(S, R)."),
        SL_CALL_AT("local-name-and-arity", "src/cowboy_req.erl", "cowboy_req.reply",
                   "do_reply(S, R)", 0, "src/cowboy_req.erl", "cowboy_req.do_reply", "Function", 7,
                   "do_reply(S, R) -> do_reply(S - 1, R)."),
        SL_CALL_AT("recursive-clause", "src/cowboy_req.erl", "cowboy_req.do_reply",
                   "do_reply(S - 1, R)", 0, "src/cowboy_req.erl", "cowboy_req.do_reply",
                   "Function", 7, "do_reply(S, R) -> do_reply(S - 1, R)."),
        SL_CALL_AT("macro-module-self-call", "src/cowboy_req.erl", "cowboy_req.set",
                   "?MODULE:reply(X, X)", 0, "src/cowboy_req.erl", "cowboy_req.reply", "Function",
                   4, "reply(S, R) -> do_reply(S, R)."),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("function-not-exported", "src/caller.erl", "caller.no_export",
                   "cowboy_req:hidden(X)", 0),
        SL_DENY_AT("variable-module", "src/caller.erl", "caller.no_module", "M:reply(X, X)", 0),
        SL_DENY_AT("variable-function", "src/caller.erl", "caller.no_function", "cowboy_req:F(X)",
                   0),
        SL_DENY_AT("import-is-for-another-arity", "src/caller.erl", "caller.no_arity", "reply(X)",
                   0),
        SL_DENY_AT("same-name-in-unimported-module", "src/caller.erl", "caller.no_decoy", "set(X)",
                   0),
        SL_DENY_AT("fun-value", "src/caller.erl", "caller.no_fun", "F(X)", 1),
        SL_DENY_AT("absent-from-named-module", "src/caller.erl", "caller.no_absent",
                   "cowboy_req:absent(X)", 0),
    };
    ASSERT(sl_check("erlang-rules", ".erl", "-module(filler).\n-export([f/0]).\nf() -> 1.\n",
                    sources, sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Zig ───────────────────────────────────────────────────────────── */

static const char ZIG_MAIN[] =
    "const std = @import(\"std\");\n"
    "const zls = @import(\"zls\");\n"
    "const other = @import(\"sub/other.zig\");\n"
    "const Thing = other.Thing;\n"
    "const aliased = other.helper;\n"
    "const Self = @This();\n"
    "\n"
    "field: *Thing,\n"
    "\n"
    "fn local() void {}\n"
    "pub fn callsLocal() void { local(); }\n"
    "pub fn callsImport() void { other.helper(); }\n"
    "pub fn callsAlias() void { aliased(); }\n"
    "pub fn callsParam(t: *const Thing) void { t.go(); }\n"
    "pub fn callsSelf(self: *Self) void { self.mine(); }\n"
    "fn mine(self: *Self) void { _ = self; }\n"
    "pub fn callsField(self: *Self) void { self.field.go(); }\n"
    "pub fn callsReturn() void { Thing.init().go(); }\n"
    "pub fn callsTyped() void { var v: Thing = undefined; v.go(); }\n"
    "pub fn callsLiteral() void { const w = Thing{}; w.go(); }\n"
    "pub fn callsTry() !void { const x = try Thing.make(); x.go(); }\n"
    "pub const Nested = struct {\n"
    "    pub fn callsOuter() void { local(); }\n"
    "};\n"
    "\n"
    "pub fn noBuild() void { zls.helper(); }\n"
    "pub fn noStd() void { std.debug.print(\"x\", .{}); }\n"
    "pub fn noCapture(o: ?*Thing) void { if (o) |p| { p.decoyOnly(); } }\n"
    "pub fn noAnytype(a: anytype) void { a.go(); }\n"
    "pub fn noPrivate() void { other.hidden(); }\n"
    "pub fn noAbsent() void { other.absent(); }\n"
    "pub fn noUndeclared() void { unknown_name(); }\n"
    "pub fn noGeneric() void { Box(u8).make(); }\n"
    "fn Box(comptime T: type) type { return struct { pub fn make() void {} }; }\n";
static const char ZIG_OTHER[] = "pub fn helper() void {}\n"
                                "fn hidden() void {}\n"
                                "pub const Thing = struct {\n"
                                "    pub fn init() Thing { return .{}; }\n"
                                "    pub fn make() !Thing { return .{}; }\n"
                                "    pub fn go(self: Thing) void { _ = self; }\n"
                                "};\n";
static const char ZIG_DECOY[] = "pub fn helper() void {}\n"
                                "pub fn local() void {}\n"
                                "pub fn unknown_name() void {}\n"
                                "pub fn decoyOnly() void {}\n";
static const char ZIG_SHARED[] = "const A = struct {\n"
                                 "    pub fn same() void {}\n"
                                 "    pub fn callsSame() void { same(); }\n"
                                 "};\n"
                                 "const B = struct {\n"
                                 "    pub fn same() void {}\n"
                                 "};\n";

#define ZIG_FILE "src/main.zig"
#define ZIG_THING_GO "    pub fn go(self: Thing) void { _ = self; }"
#define ZIG_CALL(id, method, text, tfile, tsuffix, tline, tdecl)                               \
    SL_CALL_AT(id, ZIG_FILE, "main." method, text, 0, tfile, tsuffix, "Function", tline, tdecl)
#define ZIG_GO(id, method, text)                                                               \
    ZIG_CALL(id, method, text, "src/sub/other.zig", "other.go", 6, ZIG_THING_GO)
#define ZIG_DENY(id, method, text) SL_DENY_AT(id, ZIG_FILE, "main." method, text, 0)

TEST(resolver_source_zig_containers_imports_and_declared_receivers) {
    static const RQ_Source sources[] = {
        {.path = ZIG_FILE, .text = ZIG_MAIN},
        {.path = "src/sub/other.zig", .text = ZIG_OTHER},
        {.path = "src/decoy.zig", .text = ZIG_DECOY},
        {.path = "src/shared.zig", .text = ZIG_SHARED},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("same-file-container-function", ZIG_FILE, "main.callsLocal", "local()", 1,
                   ZIG_FILE, "main.local", "Function", 10, "fn local() void {}"),
        ZIG_CALL("relative-import-pub-function", "callsImport", "other.helper()",
                 "src/sub/other.zig", "other.helper", 1, "pub fn helper() void {}"),
        ZIG_CALL("const-alias-of-imported-function", "callsAlias", "aliased()",
                 "src/sub/other.zig", "other.helper", 1, "pub fn helper() void {}"),
        ZIG_GO("declared-pointer-parameter", "callsParam", "t.go()"),
        ZIG_CALL("self-through-at-this", "callsSelf", "self.mine()", ZIG_FILE, "main.mine", 16,
                 "fn mine(self: *Self) void { _ = self; }"),
        ZIG_GO("declared-field-type", "callsField", "self.field.go()"),
        ZIG_GO("declared-return-type", "callsReturn", "Thing.init().go()"),
        ZIG_CALL("container-function-through-alias", "callsReturn", "Thing.init()",
                 "src/sub/other.zig", "other.init", 4, "    pub fn init() Thing { return .{}; }"),
        ZIG_GO("declared-local", "callsTyped", "v.go()"),
        ZIG_GO("struct-literal-local", "callsLiteral", "w.go()"),
        ZIG_GO("error-union-return-through-try", "callsTry", "x.go()"),
        ZIG_CALL("container-function-with-error-union", "callsTry", "Thing.make()",
                 "src/sub/other.zig", "other.make", 5, "    pub fn make() !Thing { return .{}; }"),
        SL_CALL_AT("nested-container-sees-outer", ZIG_FILE, "main.callsOuter", "local()", 2,
                   ZIG_FILE, "main.local", "Function", 10, "fn local() void {}"),
    };
    static const RQ_Forbidden forbidden[] = {
        ZIG_DENY("build-mapped-import", "noBuild", "zls.helper()"),
        ZIG_DENY("standard-library", "noStd", "std.debug.print(\"x\", .{})"),
        ZIG_DENY("capture-type-is-not-modelled", "noCapture", "p.decoyOnly()"),
        ZIG_DENY("anytype-has-no-container", "noAnytype", "a.go()"),
        ZIG_DENY("non-pub-from-another-file", "noPrivate", "other.hidden()"),
        ZIG_DENY("absent-from-named-file", "noAbsent", "other.absent()"),
        ZIG_DENY("name-in-unimported-file", "noUndeclared", "unknown_name()"),
        ZIG_DENY("comptime-generic-result", "noGeneric", "Box(u8).make()"),
        SL_DENY_AT("graph-node-shared-by-two-structs", "src/shared.zig", "shared.callsSame",
                   "same()", 1),
    };
    ASSERT(sl_check("zig-rules", ".zig", "pub fn f() void {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Perl ──────────────────────────────────────────────────────────── */

static const char PL_BASE[] = "package Foo::Base;\n"
                              "use strict;\n"
                              "sub new {\n"
                              "  my $class = shift;\n"
                              "  return bless {}, $class;\n"
                              "}\n"
                              "sub base_m { return 1; }\n"
                              "1;\n";
static const char PL_UTIL[] = "package Foo::Util;\n"
                              "use Exporter qw(import);\n"
                              "our @EXPORT_OK = (qw(exported));\n"
                              "sub exported { return 1; }\n"
                              "sub not_exported { return 2; }\n"
                              "1;\n";
static const char PL_DECOY[] = "package Foo::Decoy;\n"
                               "sub exported { return 3; }\n"
                               "sub helper { return 4; }\n"
                               "sub in_decoy_only { return 5; }\n"
                               "1;\n";
static const char PL_MAIN[] =
    "package Foo::Main;\n"
    "use strict;\n"
    "use parent -norequire, 'Foo::Base';\n"
    "use Foo::Util qw(exported not_exported);\n"
    "\n"
    "sub helper { return 1; }\n"
    "sub own_m { return 2; }\n"
    "sub length { return 3; }\n"
    "sub replaced { return 4; }\n"
    "*replaced = sub { return 5; };\n"
    "\n"
    "sub bare { return helper(1); }\n"
    "sub ampersand { return &helper(1); }\n"
    "sub qualified { return Foo::Util::not_exported(); }\n"
    "sub imported { return exported(); }\n"
    "sub self_own { my $self = shift; return $self->own_m; }\n"
    "sub self_list { my ($self, @rest) = @_; return $self->own_m; }\n"
    "sub self_inherited { my $self = shift; return $self->base_m; }\n"
    "sub class_new { return Foo::Main->new; }\n"
    "sub class_direct { return Foo::Base->base_m; }\n"
    "sub own_package { return __PACKAGE__->base_m; }\n"
    "sub via_super { my $self = shift; return $self->SUPER::new; }\n"
    "sub builtin_sigil { return &length('x'); }\n"
    "\n"
    "sub no_builtin { return length('x'); }\n"
    "sub no_import { return not_exported(); }\n"
    "sub no_other_package { return in_decoy_only(); }\n"
    "sub no_replaced { return replaced(); }\n"
    "sub no_other_invocant { my $other = shift; return $other->own_m; }\n"
    "sub no_rebound { my $self = shift; $self = Foo::Base->new; return $self->own_m; }\n"
    "sub no_external { return Some::Outside->new; }\n"
    "sub no_dynamic { my $self = shift; my $m = 'own_m'; return $self->$m; }\n"
    "1;\n";

#define PL_FILE "lib/Foo/Main.pm"
#define PL_CALL(id, method, text, tfile, tsuffix, tline, tdecl)                                \
    SL_CALL_AT(id, PL_FILE, "Main." method, text, 0, tfile, tsuffix, "Function", tline, tdecl)
#define PL_DENY(id, method, text) SL_DENY_AT(id, PL_FILE, "Main." method, text, 0)

TEST(resolver_source_perl_packages_exporter_and_method_resolution) {
    static const RQ_Source sources[] = {
        {.path = "lib/Foo/Base.pm", .text = PL_BASE},
        {.path = "lib/Foo/Util.pm", .text = PL_UTIL},
        {.path = "lib/Foo/Decoy.pm", .text = PL_DECOY},
        {.path = PL_FILE, .text = PL_MAIN},
    };
    static const RQ_Expected expected[] = {
        PL_CALL("current-package-sub", "bare", "helper(1)", PL_FILE, "Main.helper", 6,
                "sub helper { return 1; }"),
        PL_CALL("ampersand-form", "ampersand", "&helper(1)", PL_FILE, "Main.helper", 6,
                "sub helper { return 1; }"),
        PL_CALL("qualified-sub-issue-479", "qualified", "Foo::Util::not_exported()",
                "lib/Foo/Util.pm", "Util.not_exported", 5, "sub not_exported { return 2; }"),
        SL_CALL_AT("exporter-import", PL_FILE, "Main.imported", "exported()", 1, "lib/Foo/Util.pm",
                   "Util.exported", "Function", 4, "sub exported { return 1; }"),
        PL_CALL("self-own-method", "self_own", "$self->own_m", PL_FILE, "Main.own_m", 7,
                "sub own_m { return 2; }"),
        SL_CALL_AT("self-from-argument-list", PL_FILE, "Main.self_list", "$self->own_m", 1, PL_FILE,
                   "Main.own_m", "Function", 7, "sub own_m { return 2; }"),
        PL_CALL("self-inherited-through-parent", "self_inherited", "$self->base_m",
                "lib/Foo/Base.pm", "Base.base_m", 7, "sub base_m { return 1; }"),
        PL_CALL("class-method-new-through-parent", "class_new", "Foo::Main->new", "lib/Foo/Base.pm",
                "Base.new", 3, "sub new {"),
        PL_CALL("class-method-direct", "class_direct", "Foo::Base->base_m", "lib/Foo/Base.pm",
                "Base.base_m", 7, "sub base_m { return 1; }"),
        PL_CALL("package-token-invocant", "own_package", "__PACKAGE__->base_m", "lib/Foo/Base.pm",
                "Base.base_m", 7, "sub base_m { return 1; }"),
        PL_CALL("super-is-the-parent", "via_super", "$self->SUPER::new", "lib/Foo/Base.pm",
                "Base.new", 3, "sub new {"),
        PL_CALL("ampersand-reaches-sub-named-as-builtin", "builtin_sigil", "&length('x')", PL_FILE,
                "Main.length", 8, "sub length { return 3; }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("builtin-not-overridden-by-a-sub", PL_FILE, "Main.no_builtin", "length('x')", 1),
        SL_DENY_AT("not-in-exporter-list", PL_FILE, "Main.no_import", "not_exported()", 1),
        PL_DENY("same-name-in-another-package", "no_other_package", "in_decoy_only()"),
        PL_DENY("glob-replaces-the-sub", "no_replaced", "replaced()"),
        PL_DENY("not-the-invocant-convention", "no_other_invocant", "$other->own_m"),
        SL_DENY_AT("invocant-rebound", PL_FILE, "Main.no_rebound", "$self->own_m", 2),
        PL_DENY("package-outside-the-project", "no_external", "Some::Outside->new"),
        PL_DENY("computed-method-name", "no_dynamic", "$self->$m"),
    };
    ASSERT(sl_check("perl-rules", ".pm", "package Filler;\nsub f { return 1; }\n1;\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Ruby ──────────────────────────────────────────────────────────── */

static const char RB_BASE[] = "module Sinatra\n"
                              "  module Helpers\n"
                              "    def from_helpers\n"
                              "      1\n"
                              "    end\n"
                              "  end\n"
                              "\n"
                              "  class Base\n"
                              "    include Helpers\n"
                              "\n"
                              "    class << self\n"
                              "      def get(path)\n"
                              "        route('GET', path)\n"
                              "      end\n"
                              "\n"
                              "      def route(verb, path)\n"
                              "        verb\n"
                              "      end\n"
                              "    end\n"
                              "\n"
                              "    def self.settings\n"
                              "      self\n"
                              "    end\n"
                              "\n"
                              "    def initialize(app)\n"
                              "      @app = app\n"
                              "    end\n"
                              "\n"
                              "    def own\n"
                              "      1\n"
                              "    end\n"
                              "\n"
                              "    def call_own\n"
                              "      own()\n"
                              "    end\n"
                              "\n"
                              "    def call_self\n"
                              "      self.own()\n"
                              "    end\n"
                              "\n"
                              "    def call_mixin\n"
                              "      from_helpers()\n"
                              "    end\n"
                              "\n"
                              "    def call_top\n"
                              "      top_level()\n"
                              "    end\n"
                              "\n"
                              "    def call_constant\n"
                              "      Base.settings\n"
                              "    end\n"
                              "\n"
                              "    def call_new\n"
                              "      Sinatra::Base.new(1)\n"
                              "    end\n"
                              "\n"
                              "    def call_local\n"
                              "      made = Other.new\n"
                              "      made.other_m()\n"
                              "    end\n"
                              "\n"
                              "    def call_in_block\n"
                              "      [1].each { |x| own() }\n"
                              "    end\n"
                              "\n"
                              "    def no_unknown_receiver(thing)\n"
                              "      thing.own()\n"
                              "    end\n"
                              "\n"
                              "    def no_singleton_from_instance\n"
                              "      route('GET', '/')\n"
                              "    end\n"
                              "\n"
                              "    def no_reassigned\n"
                              "      made = Other.new\n"
                              "      made = 1\n"
                              "      made.other_m()\n"
                              "    end\n"
                              "\n"
                              "    def no_outside\n"
                              "      Outside.settings\n"
                              "    end\n"
                              "\n"
                              "    get '/' do\n"
                              "      own()\n"
                              "    end\n"
                              "  end\n"
                              "\n"
                              "  class Other\n"
                              "    def other_m\n"
                              "      2\n"
                              "    end\n"
                              "\n"
                              "    def own\n"
                              "      3\n"
                              "    end\n"
                              "  end\n"
                              "\n"
                              "  class Child < Base\n"
                              "    def call_inherited\n"
                              "      own()\n"
                              "    end\n"
                              "  end\n"
                              "end\n"
                              "\n"
                              "def top_level\n"
                              "  1\n"
                              "end\n";

#define RB_FILE "lib/sinatra/base.rb"
#define RB_CALL(id, caller_name, text, tsuffix, tline, tdecl)                                  \
    SL_CALL_AT(id, RB_FILE, caller_name, text, 0, RB_FILE, tsuffix, "Method", tline, tdecl)
#define RB_OWN "    def own"

TEST(resolver_source_ruby_ancestor_chain_and_self_rules) {
    static const RQ_Source sources[] = {{.path = RB_FILE, .text = RB_BASE}};
    static const RQ_Expected expected[] = {
        RB_CALL("singleton-method-from-class-self", "Base.get", "route('GET', path)", "Base.route",
                16, "      def route(verb, path)"),
        RB_CALL("implicit-self-own-method", "Base.call_own", "own()", "Base.own", 29, RB_OWN),
        RB_CALL("explicit-self", "Base.call_self", "self.own()", "Base.own", 29, RB_OWN),
        RB_CALL("included-module-method", "Base.call_mixin", "from_helpers()",
                "Helpers.from_helpers", 3, "    def from_helpers"),
        SL_CALL_AT("object-is-on-every-chain", RB_FILE, "Base.call_top", "top_level()", 0, RB_FILE,
                   "base.top_level", "Function", 106, "def top_level"),
        RB_CALL("constant-singleton-method", "Base.call_constant", "Base.settings", "Base.settings",
                21, "    def self.settings"),
        RB_CALL("new-selects-initialize", "Base.call_new", "Sinatra::Base.new(1)",
                "Base.initialize", 25, "    def initialize(app)"),
        RB_CALL("local-assigned-once-from-new", "Base.call_local", "made.other_m()",
                "Other.other_m", 90, "    def other_m"),
        SL_CALL_AT("block-keeps-self-in-a-method", RB_FILE, "Base.call_in_block", "own()", 2,
                   RB_FILE, "Base.own", "Method", 29, RB_OWN),
        SL_CALL_AT("superclass-method", RB_FILE, "Child.call_inherited", "own()", 5, RB_FILE,
                   "Base.own", "Method", 29, RB_OWN),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("receiver-of-unknown-class", RB_FILE, "Base.no_unknown_receiver", "thing.own()",
                   0),
        SL_DENY_AT("singleton-method-is-not-on-instances", RB_FILE,
                   "Base.no_singleton_from_instance", "route('GET', '/')", 0),
        SL_DENY_AT("local-assigned-twice", RB_FILE, "Base.no_reassigned", "made.other_m()", 1),
        SL_DENY_AT("constant-outside-the-project", RB_FILE, "Base.no_outside", "Outside.settings",
                   0),
    };
    ASSERT(sl_check("ruby-rules", ".rb", "def filler\n  1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Dart ──────────────────────────────────────────────────────────── */

static const char DART_MAIN[] =
    "import 'package:counter/barrel.dart';\n"
    "import 'src/store.dart' as s show helper;\n"
    "part 'main_part.dart';\n"
    "\n"
    "class Counter extends Base with Mix, Later {\n"
    "  final Store store;\n"
    "  final made = Store();\n"
    "  Counter(this.store);\n"
    "  void callsOwn() { ownM(); }\n"
    "  void callsInherited() { fromBase(); }\n"
    "  void callsMixin() { fromMix(); }\n"
    "  void callsLastMixin() { both(); }\n"
    "  void callsLibraryFirst() { fromLibrary(); }\n"
    "  void callsField() { store.save(); }\n"
    "  void callsInferred() { made.save(); }\n"
    "  void callsParam(Store p) { p.save(); }\n"
    "  void callsLocal() { final l = Store(); l.save(); }\n"
    "  void callsTyped() { Store t = Store.named(); t.save(); }\n"
    "  void callsStatic() { Store.make(); }\n"
    "  void callsTop() { topLocal(); }\n"
    "  void callsBarrel() { helper(); }\n"
    "  void callsPrefix() { s.helper(); }\n"
    "  void callsPart() { fromPart(); }\n"
    "  void callsNew() { new Store(); }\n"
    "  void noShadow(void Function() ownM) { ownM(); }\n"
    "  void noLocalFn() { void topLocal() {} topLocal(); }\n"
    "  void noHidden() { hiddenName(); }\n"
    "  void noPrivate() { _private(); }\n"
    "  void noUnimported() { unimported(); }\n"
    "  void noUnknown(dynamic d) { d.save(); }\n"
    "  void noUntyped(x) { x.save(); }\n"
    "  void noPrefixShow() { s.notShown(); }\n"
    "  void noClosureParam() { [1].map((ownM) => ownM()); }\n"
    "  void noValueCall() { store(); }\n"
    "  void ownM() {}\n"
    "}\n"
    "void fromLibrary() {}\n"
    "void topLocal() {}\n";
static const char DART_PART[] = "part of 'main.dart';\n"
                                "void fromPart() {}\n"
                                "void partCalls() { topLocal(); }\n";
static const char DART_BARREL[] = "export 'src/store.dart' hide hiddenName;\n"
                                  "export 'src/base.dart';\n";
static const char DART_STORE[] = "class Store {\n"
                                 "  Store();\n"
                                 "  Store.named();\n"
                                 "  static Store make() => Store();\n"
                                 "  void save() {}\n"
                                 "}\n"
                                 "void helper() {}\n"
                                 "void notShown() {}\n"
                                 "void _private() {}\n"
                                 "void hiddenName() {}\n";
static const char DART_BASE[] = "abstract class Base {\n"
                                "  void fromBase() {}\n"
                                "  void fromLibrary() {}\n"
                                "}\n"
                                "mixin Mix {\n"
                                "  void fromMix() {}\n"
                                "  void both() {}\n"
                                "}\n"
                                "mixin Later {\n"
                                "  void both() {}\n"
                                "}\n";
static const char DART_DECOY[] = "class Other {\n"
                                 "  void save() {}\n"
                                 "}\n"
                                 "void unimported() {}\n";
static const char DART_EXT[] = "import 'package:flutter/material.dart';\n"
                               "import 'src/base.dart';\n"
                               "class Ext extends Base {\n"
                               "  void callsBase() { fromBase(); }\n"
                               "}\n"
                               "class W extends StatelessWidget {\n"
                               "  void callsOutside() { build(); }\n"
                               "  void callsOwnW() { ownW(); }\n"
                               "  void ownW() {}\n"
                               "}\n";

#define DART_FILE "lib/main.dart"
#define DART_STORE_FILE "lib/src/store.dart"
#define DART_BASE_FILE "lib/src/base.dart"
#define DART_CALL(id, method, text, tfile, tsuffix, label, tline, tdecl)                       \
    SL_CALL_AT(id, DART_FILE, "Counter." method, text, 0, tfile, tsuffix, label, tline, tdecl)
#define DART_DENY(id, method, text, at) SL_DENY_AT(id, DART_FILE, "Counter." method, text, at)
#define DART_SAVE(id, method, text)                                                            \
    DART_CALL(id, method, text, DART_STORE_FILE, "Store.save", "Method", 5, "  void save() {}")
#define DART_STORE_CLASS(id, method, text)                                                     \
    DART_CALL(id, method, text, DART_STORE_FILE, "store.Store", "Class", 1, "class Store {")

TEST(resolver_source_dart_lexical_scope_libraries_and_receivers) {
    static const RQ_Source sources[] = {
        {.path = "pubspec.yaml", .text = "name: counter\n"},
        {.path = DART_FILE, .text = DART_MAIN},
        {.path = "lib/main_part.dart", .text = DART_PART},
        {.path = "lib/barrel.dart", .text = DART_BARREL},
        {.path = DART_STORE_FILE, .text = DART_STORE},
        {.path = DART_BASE_FILE, .text = DART_BASE},
        {.path = "lib/decoy.dart", .text = DART_DECOY},
        {.path = "lib/ext.dart", .text = DART_EXT},
    };
    static const RQ_Expected expected[] = {
        DART_CALL("own-member", "callsOwn", "ownM()", DART_FILE, "Counter.ownM", "Method", 35,
                  "  void ownM() {}"),
        DART_CALL("inherited-member-with-no-outside-import", "callsInherited", "fromBase()",
                  DART_BASE_FILE, "Base.fromBase", "Method", 2, "  void fromBase() {}"),
        DART_CALL("mixin-member", "callsMixin", "fromMix()", DART_BASE_FILE, "Mix.fromMix",
                  "Method", 6, "  void fromMix() {}"),
        DART_CALL("last-mixin-wins", "callsLastMixin", "both()", DART_BASE_FILE, "Later.both",
                  "Method", 10, "  void both() {}"),
        DART_CALL("library-function-beats-inherited-member", "callsLibraryFirst", "fromLibrary()",
                  DART_FILE, "main.fromLibrary", "Function", 38, "void fromLibrary() {}"),
        DART_SAVE("declared-field-receiver", "callsField", "store.save"),
        DART_SAVE("field-initialized-by-constructor", "callsInferred", "made.save"),
        DART_SAVE("declared-parameter-receiver", "callsParam", "p.save"),
        DART_STORE_CLASS("constructor-call", "callsLocal", "Store()"),
        DART_SAVE("local-initialized-by-constructor", "callsLocal", "l.save"),
        DART_STORE_CLASS("named-constructor", "callsTyped", "Store.named"),
        DART_SAVE("declared-local-receiver", "callsTyped", "t.save"),
        DART_CALL("static-method-on-class", "callsStatic", "Store.make", DART_STORE_FILE,
                  "Store.make", "Method", 4, "  static Store make() => Store();"),
        DART_CALL("library-top-level", "callsTop", "topLocal()", DART_FILE, "main.topLocal",
                  "Function", 39, "void topLocal() {}"),
        DART_CALL("through-package-uri-and-export", "callsBarrel", "helper()", DART_STORE_FILE,
                  "store.helper", "Function", 7, "void helper() {}"),
        DART_CALL("prefix-with-show", "callsPrefix", "s.helper", DART_STORE_FILE, "store.helper",
                  "Function", 7, "void helper() {}"),
        DART_CALL("function-in-a-part", "callsPart", "fromPart()", "lib/main_part.dart",
                  "main_part.fromPart", "Function", 2, "void fromPart() {}"),
        DART_STORE_CLASS("new-expression", "callsNew", "new Store()"),
        SL_CALL_AT("part-sees-its-library", "lib/main_part.dart", "main_part.partCalls",
                   "topLocal()", 0, DART_FILE, "main.topLocal", "Function", 39,
                   "void topLocal() {}"),
        SL_CALL_AT("own-member-under-an-outside-superclass", "lib/ext.dart", "W.callsOwnW",
                   "ownW()", 0, "lib/ext.dart", "W.ownW", "Method", 9, "  void ownW() {}"),
    };
    static const RQ_Forbidden forbidden[] = {
        DART_DENY("parameter-shadows-member", "noShadow", "ownM()", 1),
        DART_DENY("local-function-shadows-library", "noLocalFn", "topLocal()", 1),
        DART_DENY("hidden-by-the-export", "noHidden", "hiddenName()", 0),
        DART_DENY("private-to-its-library", "noPrivate", "_private()", 0),
        DART_DENY("same-project-but-not-imported", "noUnimported", "unimported()", 0),
        DART_DENY("dynamic-receiver", "noUnknown", "d.save", 0),
        DART_DENY("untyped-parameter", "noUntyped", "x.save", 0),
        DART_DENY("show-list-excludes-it", "noPrefixShow", "s.notShown", 0),
        DART_DENY("closure-parameter-shadows-member", "noClosureParam", "ownM()", 2),
        DART_DENY("call-of-a-value", "noValueCall", "store()", 0),
        SL_DENY_AT("outside-import-can-win-over-inherited", "lib/ext.dart", "Ext.callsBase",
                   "fromBase()", 0),
        SL_DENY_AT("superclass-outside-project", "lib/ext.dart", "W.callsOutside", "build()", 0),
    };
    ASSERT(sl_check("dart-rules", ".dart", "void filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Swift ─────────────────────────────────────────────────────────── */

static const char SW_MANIFEST[] = "// swift-tools-version:5.9\n"
                                  "import PackageDescription\n"
                                  "let package = Package(name: \"App\", targets: [\n"
                                  "    .target(name: \"App\", dependencies: [\"Lib\"]),\n"
                                  "    .target(name: \"Lib\", path: \"Sources/Lib\"),\n"
                                  "    .testTarget(name: \"AppTests\", dependencies: [\"App\"]),\n"
                                  "])\n";
static const char SW_MAIN[] =
    "import Lib\n"
    "\n"
    "func sameFile() {}\n"
    "private func fileHelper() {}\n"
    "func byLabel(a: Int) {}\n"
    "struct Box {\n"
    "    func put() {}\n"
    "    static func make() -> Box { Box() }\n"
    "}\n"
    "class Base {\n"
    "    func fromBase() {}\n"
    "}\n"
    "protocol Greets {\n"
    "    func greet()\n"
    "}\n"
    "extension Greets {\n"
    "    func wave() {}\n"
    "}\n"
    "class Service: Base, Greets {\n"
    "    let box: Box\n"
    "    var maybe: Box?\n"
    "    override init() { box = Box() }\n"
    "    func greet() {}\n"
    "    func ownM() {}\n"
    "    func callsOwn() { ownM() }\n"
    "    func callsSelf() { self.ownM() }\n"
    "    func callsInherited() { fromBase() }\n"
    "    func callsDefault() { wave() }\n"
    "    func callsExtension() { inExtension() }\n"
    "    func callsProperty() { box.put() }\n"
    "    func callsOptional() { maybe?.put() }\n"
    "    func callsParam(p: Box) { p.put() }\n"
    "    func callsLocal() { let l = Box(); l.put() }\n"
    "    func callsStatic() { Box.make() }\n"
    "    func callsResult() { Box.make().put() }\n"
    "    func callsFree() { sameFile() }\n"
    "    func callsPrivate() { fileHelper() }\n"
    "    func callsOtherFile() { otherFile() }\n"
    "    func callsLabelA() { byLabel(a: 1) }\n"
    "    func callsLabelB() { byLabel(b: 1) }\n"
    "    func callsImported() { libPublic() }\n"
    "    func callsQualified() { Lib.libPublic() }\n"
    "    func callsImportedType() { LibType().run() }\n"
    "    func noParamShadow(ownM: () -> Void) { ownM() }\n"
    "    func localShadowsProperty() { let box = Other(); box.put() }\n"
    "    func noInternalOfImport() { libInternal() }\n"
    "    func noPrivateElsewhere() { otherPrivate() }\n"
    "    func noDynamic(x: Any) { x.put() }\n"
    "    func noGeneric<T>(t: T) { t.put() }\n"
    "    func noClosureParam() { [1].map { ownM in ownM() } }\n"
    "    func noValueCall() { box() }\n"
    "    func noCaseBinding(o: Box?) { switch o { case .some(let box): box.put(); default: break } "
    "}\n"
    "}\n"
    "struct Other { func put() {} }\n"
    "class Outside: NSObject {\n"
    "    func own() {}\n"
    "    func callsOwnMember() { own() }\n"
    "    func noFreeUnderOpenBase() { sameFile() }\n"
    "    func noInherited() { inheritedFromOutside() }\n"
    "}\n";
static const char SW_SECOND[] = "func otherFile() {}\n"
                                "func byLabel(b: Int) {}\n"
                                "private func otherPrivate() {}\n"
                                "extension Service {\n"
                                "    func inExtension() {}\n"
                                "}\n";
static const char SW_LIB[] = "public func libPublic() {}\n"
                             "func libInternal() {}\n"
                             "public class LibType {\n"
                             "    public init() {}\n"
                             "    public func run() {}\n"
                             "}\n";
static const char SW_TEST[] = "@testable import App\n"
                              "func testCalls() { sameFile() }\n";
static const char SW_LOOSE[] = "func looseLocal() {}\n"
                               "func looseCalls() { looseLocal(); loose2() }\n";

#define SW_FILE "Sources/App/Main.swift"
#define SW_SECOND_FILE "Sources/App/Second.swift"
#define SW_LIB_FILE "Sources/Lib/Lib.swift"
#define SW_CALL(id, method, text, at, tfile, tsuffix, label, tline, tdecl)                     \
    SL_CALL_AT(id, SW_FILE, "Service." method, text, at, tfile, tsuffix, label, tline, tdecl)
#define SW_DENY(id, method, text, at) SL_DENY_AT(id, SW_FILE, "Service." method, text, at)
#define SW_PUT(id, method, text)                                                               \
    SW_CALL(id, method, text, 0, SW_FILE, "Box.put", "Method", 7, "    func put() {}")
#define SW_BOX(id, method, at)                                                                 \
    SW_CALL(id, method, "Box()", at, SW_FILE, "Main.Box", "Struct", 6, "struct Box {")

TEST(resolver_source_swift_modules_lookup_and_receivers) {
    static const RQ_Source sources[] = {
        {.path = "Package.swift", .text = SW_MANIFEST},
        {.path = SW_FILE, .text = SW_MAIN},
        {.path = SW_SECOND_FILE, .text = SW_SECOND},
        {.path = SW_LIB_FILE, .text = SW_LIB},
        {.path = "Tests/AppTests/T.swift", .text = SW_TEST},
        {.path = "Scripts/loose.swift", .text = SW_LOOSE},
        {.path = "Scripts/loose2.swift", .text = "func loose2() {}\n"},
    };
    static const RQ_Expected expected[] = {
        SW_CALL("own-member", "callsOwn", "ownM()", 1, SW_FILE, "Service.ownM", "Method", 24,
                "    func ownM() {}"),
        SW_CALL("explicit-self", "callsSelf", "self.ownM()", 0, SW_FILE, "Service.ownM", "Method",
                24, "    func ownM() {}"),
        SW_CALL("superclass-member", "callsInherited", "fromBase()", 1, SW_FILE, "Base.fromBase",
                "Method", 11, "    func fromBase() {}"),
        SW_CALL("protocol-extension-member", "callsDefault", "wave()", 1, SW_FILE, "Greets.wave",
                "Method", 17, "    func wave() {}"),
        SW_CALL("extension-in-another-file", "callsExtension", "inExtension()", 0, SW_SECOND_FILE,
                "Service.inExtension", "Method", 5, "    func inExtension() {}"),
        SW_PUT("declared-property-receiver", "callsProperty", "box.put()"),
        SW_PUT("optional-chain-keeps-the-type", "callsOptional", "maybe?.put()"),
        SW_PUT("declared-parameter-receiver", "callsParam", "p.put()"),
        SW_BOX("initializer-call", "callsLocal", 2),
        SW_PUT("local-from-initializer", "callsLocal", "l.put()"),
        SW_CALL("static-method", "callsStatic", "Box.make()", 0, SW_FILE, "Box.make", "Method", 8,
                "    static func make() -> Box { Box() }"),
        SW_CALL("result-of-a-declared-return-type", "callsResult", "Box.make()", 1, SW_FILE,
                "Box.make", "Method", 8, "    static func make() -> Box { Box() }"),
        SW_PUT("member-on-a-declared-return-type", "callsResult", "Box.make().put()"),
        SW_CALL("same-file-function", "callsFree", "sameFile()", 1, SW_FILE, "Main.sameFile",
                "Function", 3, "func sameFile() {}"),
        SW_CALL("private-in-its-file", "callsPrivate", "fileHelper()", 1, SW_FILE,
                "Main.fileHelper", "Function", 4, "private func fileHelper() {}"),
        SW_CALL("another-file-of-the-target", "callsOtherFile", "otherFile()", 0, SW_SECOND_FILE,
                "Second.otherFile", "Function", 1, "func otherFile() {}"),
        SW_CALL("label-selects-this-overload", "callsLabelA", "byLabel(a: 1)", 0, SW_FILE,
                "Main.byLabel", "Function", 5, "func byLabel(a: Int) {}"),
        SW_CALL("label-selects-that-overload", "callsLabelB", "byLabel(b: 1)", 0, SW_SECOND_FILE,
                "Second.byLabel", "Function", 2, "func byLabel(b: Int) {}"),
        SW_CALL("public-of-an-imported-target", "callsImported", "libPublic()", 0, SW_LIB_FILE,
                "Lib.libPublic", "Function", 1, "public func libPublic() {}"),
        SW_CALL("module-qualified", "callsQualified", "Lib.libPublic()", 0, SW_LIB_FILE,
                "Lib.libPublic", "Function", 1, "public func libPublic() {}"),
        SW_CALL("imported-type", "callsImportedType", "LibType()", 0, SW_LIB_FILE, "Lib.LibType",
                "Class", 3, "public class LibType {"),
        SW_CALL("member-of-imported-type", "callsImportedType", "LibType().run()", 0, SW_LIB_FILE,
                "LibType.run", "Method", 5, "    public func run() {}"),
        SW_CALL("local-shadows-property-with-its-own-type", "localShadowsProperty", "box.put()", 1,
                SW_FILE, "Other.put", "Method", 56, "struct Other { func put() {} }"),
        SW_CALL("initializer-of-the-shadowing-local", "localShadowsProperty", "Other()", 0, SW_FILE,
                "Main.Other", "Struct", 56, "struct Other { func put() {} }"),
        SL_CALL_AT("own-member-under-an-outside-superclass", SW_FILE, "Outside.callsOwnMember",
                   "own()", 1, SW_FILE, "Outside.own", "Method", 58, "    func own() {}"),
        SL_CALL_AT("testable-import-reaches-internal", "Tests/AppTests/T.swift", "T.testCalls",
                   "sameFile()", 0, SW_FILE, "Main.sameFile", "Function", 3, "func sameFile() {}"),
        SL_CALL_AT("no-target-still-decides-its-own-file", "Scripts/loose.swift",
                   "loose.looseCalls", "looseLocal()", 1, "Scripts/loose.swift", "loose.looseLocal",
                   "Function", 1, "func looseLocal() {}"),
    };
    static const RQ_Forbidden forbidden[] = {
        SW_DENY("parameter-shadows-member", "noParamShadow", "ownM()", 3),
        SW_DENY("closure-parameter-shadows-member", "noClosureParam", "ownM()", 4),
        SW_DENY("internal-of-an-import-without-testable", "noInternalOfImport", "libInternal()", 0),
        SW_DENY("private-to-another-file", "noPrivateElsewhere", "otherPrivate()", 0),
        SW_DENY("any-has-no-members-here", "noDynamic", "x.put()", 0),
        SW_DENY("generic-parameter", "noGeneric", "t.put()", 0),
        SW_DENY("call-of-a-value", "noValueCall", "box()", 0),
        SW_DENY("case-binding-shadows-property", "noCaseBinding", "box.put()", 2),
        SL_DENY_AT("free-function-under-an-outside-superclass", SW_FILE,
                   "Outside.noFreeUnderOpenBase", "sameFile()", 2),
        SL_DENY_AT("member-of-an-outside-superclass", SW_FILE, "Outside.noInherited",
                   "inheritedFromOutside()", 0),
        SL_DENY_AT("no-known-module-across-files", "Scripts/loose.swift", "loose.looseCalls",
                   "loose2()", 0),
    };
    ASSERT(sl_check("swift-rules", ".swift", "func filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

SUITE(resolver_source_langs) {
    RUN_TEST(resolver_source_elixir_modules_aliases_imports_and_arity);
    RUN_TEST(resolver_source_erlang_module_arity_export_and_import);
    RUN_TEST(resolver_source_zig_containers_imports_and_declared_receivers);
    RUN_TEST(resolver_source_perl_packages_exporter_and_method_resolution);
    RUN_TEST(resolver_source_ruby_ancestor_chain_and_self_rules);
    RUN_TEST(resolver_source_dart_lexical_scope_libraries_and_receivers);
    RUN_TEST(resolver_source_swift_modules_lookup_and_receivers);
}
