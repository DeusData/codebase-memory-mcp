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
#define SL_USE_AT(id, sfile, scaller, text, at, tfile, tsuffix, tlabel, tline, tdecl)          \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "USAGE",                                                                   \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = tlabel,                                                            \
                   .declaration_line = tline,                                                  \
                   .declaration_text = tdecl},                                                 \
    }
#define SL_NO_USE_AT(id, sfile, scaller, text, at)                                             \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "USAGE", .any_target = true                                                \
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
        /* A build manifest has no node; the oracle's sources are the ones that do. */
        RQ_Source *indexed = calloc(source_count, sizeof(*indexed));
        size_t indexed_count = 0;
        for (size_t i = 0; indexed && i < source_count; i++) {
            const char *slash = strrchr(sources[i].path, '/');
            if (strcmp(slash ? slash + 1 : sources[i].path, "dune"))
                indexed[indexed_count++] = sources[i];
        }
        ok = store && indexed &&
             rq_check(store, project, project, indexed, indexed_count, expected, expected_count,
                      forbidden, forbidden_count, &report, stderr) &&
             ok;
        free(indexed);
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
    "fn Box(comptime T: type) type { return struct { pub fn make() void {} }; }\n"
    "const LIMIT = 3;\n"
    "pub fn usesConst() u8 { return LIMIT; }\n"
    "pub fn noMemberName(s: anytype) u8 { return s.LIMIT; }\n";
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
    ZIG_CALL(id, method, text, "src/sub/other.zig", "other.Thing.go", 6, ZIG_THING_GO)
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
                 "src/sub/other.zig", "other.Thing.init", 4,
                 "    pub fn init() Thing { return .{}; }"),
        ZIG_GO("declared-local", "callsTyped", "v.go()"),
        ZIG_GO("struct-literal-local", "callsLiteral", "w.go()"),
        ZIG_GO("error-union-return-through-try", "callsTry", "x.go()"),
        ZIG_CALL("container-function-with-error-union", "callsTry", "Thing.make()",
                 "src/sub/other.zig", "other.Thing.make", 5,
                 "    pub fn make() !Thing { return .{}; }"),
        SL_CALL_AT("nested-container-sees-outer", ZIG_FILE, "Nested.callsOuter", "local()", 2,
                   ZIG_FILE, "main.local", "Function", 10, "fn local() void {}"),
        SL_USE_AT("name-read-from-the-enclosing-container", ZIG_FILE, "main.usesConst", "LIMIT", 1,
                  ZIG_FILE, "main.LIMIT", "Variable", 35, "const LIMIT = 3;"),
        SL_CALL_AT("a-function-is-its-containers-own", "src/shared.zig", "A.callsSame", "same()", 1,
                   "src/shared.zig", "shared.A.same", "Function", 2, "    pub fn same() void {}"),
        ZIG_CALL("comptime-function-itself-is-a-call", "noGeneric", "Box(u8)", ZIG_FILE, "main.Box",
                 34, "fn Box(comptime T: type) type { return struct { pub fn make() void {} }; }"),
    };
    static const RQ_Forbidden forbidden[] = {
        ZIG_DENY("build-mapped-import", "noBuild", "zls.helper()"),
        ZIG_DENY("standard-library", "noStd", "std.debug.print(\"x\", .{})"),
        ZIG_DENY("capture-has-no-member-of-that-name", "noCapture", "p.decoyOnly()"),
        ZIG_DENY("anytype-has-no-container", "noAnytype", "a.go()"),
        ZIG_DENY("non-pub-from-another-file", "noPrivate", "other.hidden()"),
        ZIG_DENY("absent-from-named-file", "noAbsent", "other.absent()"),
        ZIG_DENY("name-in-unimported-file", "noUndeclared", "unknown_name()"),
        ZIG_DENY("comptime-generic-result", "noGeneric", "Box(u8).make()"),
        SL_NO_USE_AT("member-name-is-not-a-read", ZIG_FILE, "main.noMemberName", "LIMIT", 2),
    };
    ASSERT(sl_check("zig-rules", ".zig", "pub fn f() void {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

static const char ZIG_FLOW[] =
    "const other = @import(\"sub/other.zig\");\n"
    "const Thing = other.Thing;\n"
    "fn maybe() ?*Thing { return null; }\n"
    "fn fails() !Thing { return .{}; }\n"
    "fn list() ?[]Thing { return null; }\n"
    "pub fn viaOrelse() void { const a = maybe() orelse return; a.go(); }\n"
    "pub fn viaCatch() void { const b = fails() catch return; b.go(); }\n"
    "pub fn viaDeref(p: *Thing) void { const c = p.*; c.go(); }\n"
    "pub fn viaIf() void { if (maybe()) |d| { d.go(); } }\n"
    "pub fn viaWhile() void { while (maybe()) |e| { e.go(); } }\n"
    "pub fn noElse() void { if (fails()) |_| {} else |err| { err.go(); } }\n"
    "pub fn noCatchCapture() void { const q = fails() catch |bad| bad.go(); _ = q; }\n"
    "pub fn noSlice() void { if (list()) |s| { s.go(); } }\n"
    "pub fn noFor(items: []Thing) void { for (items) |i| { i.go(); } }\n"
    "pub fn noSwitch(u: anytype) void { switch (u) { .x => |v| v.go(), } }\n"
    "pub fn readsPath() u8 { return other.LIMIT; }\n"
    "pub fn readsType(x: other.Thing) void { _ = x; }\n"
    "pub fn readsFunction() void { const f = other.helper; _ = f; }\n"
    "pub fn noPrivateRead() u8 { return other.SECRET; }\n"
    "pub fn noFieldRead(t: Thing) u8 { return t.size; }\n"
    "pub fn noAnytypeRead(t: anytype) u8 { return t.LIMIT; }\n"
    "pub fn callIsNotARead() void { other.helper(); }\n"
    "pub fn readsBehindAPointer(x: *other.Thing, y: ?other.Thing) void { _ = x; _ = y; }\n";
static const char ZIG_FLOW_OTHER[] = "pub fn helper() void {}\n"
                                     "pub const LIMIT = 3;\n"
                                     "const SECRET = 4;\n"
                                     "pub const Thing = struct {\n"
                                     "    size: u8,\n"
                                     "    pub fn go(self: Thing) void { _ = self; }\n"
                                     "};\n";
static const char ZIG_FLOW_DECOY[] = "pub const size = 1;\n"
                                     "pub const SECRET = 2;\n";
#define ZIG_FLOW_FILE "src/flow.zig"
#define ZIG_FLOW_GO(id, method, text)                                                          \
    SL_CALL_AT(id, ZIG_FLOW_FILE, "flow." method, text, 0, "src/sub/other.zig",                \
               "other.Thing.go", "Function", 6, ZIG_THING_GO)
#define ZIG_FLOW_DENY(id, method, text) SL_DENY_AT(id, ZIG_FLOW_FILE, "flow." method, text, 0)

#define ZIG_FLOW_OTHER_READ(id, method, at)                                                   \
    SL_USE_AT(id, ZIG_FLOW_FILE, "flow." method, "other", at, ZIG_FLOW_FILE, "flow.other",     \
              "Variable", 1, "const other = @import(\"sub/other.zig\");")
#define ZIG_FLOW_FAILS(id, method, at)                                                        \
    SL_CALL_AT(id, ZIG_FLOW_FILE, "flow." method, "fails()", at, ZIG_FLOW_FILE, "flow.fails",  \
               "Function", 4, "fn fails() !Thing { return .{}; }")
TEST(resolver_source_zig_unwraps_captures_and_member_reads) {
    static const RQ_Source sources[] = {
        {.path = ZIG_FLOW_FILE, .text = ZIG_FLOW},
        {.path = "src/sub/other.zig", .text = ZIG_FLOW_OTHER},
        {.path = "src/decoy.zig", .text = ZIG_FLOW_DECOY},
    };
    static const RQ_Expected expected[] = {
        ZIG_FLOW_GO("orelse-keeps-the-optional-payload", "viaOrelse", "a.go()"),
        SL_CALL_AT("orelse-operand-is-still-a-call", ZIG_FLOW_FILE, "flow.viaOrelse", "maybe()", 1,
                   ZIG_FLOW_FILE, "flow.maybe", "Function", 3,
                   "fn maybe() ?*Thing { return null; }"),
        ZIG_FLOW_GO("catch-keeps-the-error-union-payload", "viaCatch", "b.go()"),
        SL_CALL_AT("catch-operand-is-still-a-call", ZIG_FLOW_FILE, "flow.viaCatch", "fails()", 1,
                   ZIG_FLOW_FILE, "flow.fails", "Function", 4, "fn fails() !Thing { return .{}; }"),
        SL_CALL_AT("if-condition-is-still-a-call", ZIG_FLOW_FILE, "flow.viaIf", "maybe()", 2,
                   ZIG_FLOW_FILE, "flow.maybe", "Function", 3,
                   "fn maybe() ?*Thing { return null; }"),
        SL_CALL_AT("while-condition-is-still-a-call", ZIG_FLOW_FILE, "flow.viaWhile", "maybe()", 3,
                   ZIG_FLOW_FILE, "flow.maybe", "Function", 3,
                   "fn maybe() ?*Thing { return null; }"),
        ZIG_FLOW_GO("dereference-keeps-the-pointee", "viaDeref", "c.go()"),
        ZIG_FLOW_GO("if-capture-unwraps-the-condition", "viaIf", "d.go()"),
        ZIG_FLOW_GO("while-capture-unwraps-the-condition", "viaWhile", "e.go()"),
        SL_USE_AT("value-through-an-import", ZIG_FLOW_FILE, "flow.readsPath", "LIMIT", 0,
                  "src/sub/other.zig", "other.LIMIT", "Variable", 2, "pub const LIMIT = 3;"),
        ZIG_FLOW_OTHER_READ("the-import-is-read-on-the-way", "readsPath", 3),
        ZIG_FLOW_OTHER_READ("the-import-is-read-for-a-private-name", "noPrivateRead", 6),
        ZIG_FLOW_OTHER_READ("the-import-is-read-for-a-call", "callIsNotARead", 7),
        ZIG_FLOW_FAILS("condition-beside-an-else-capture", "noElse", 2),
        ZIG_FLOW_FAILS("operand-beside-a-catch-capture", "noCatchCapture", 3),
        SL_CALL_AT("condition-of-a-slice-capture", ZIG_FLOW_FILE, "flow.noSlice", "list()", 1,
                   ZIG_FLOW_FILE, "flow.list", "Function", 5,
                   "fn list() ?[]Thing { return null; }"),
        SL_USE_AT("type-path-behind-a-pointer", ZIG_FLOW_FILE, "flow.readsBehindAPointer", "Thing",
                  9, "src/sub/other.zig", "other.Thing", "Variable", 4,
                  "pub const Thing = struct {"),
        SL_USE_AT("type-path-behind-an-optional", ZIG_FLOW_FILE, "flow.readsBehindAPointer",
                  "Thing", 10, "src/sub/other.zig", "other.Thing", "Variable", 4,
                  "pub const Thing = struct {"),
        ZIG_FLOW_OTHER_READ("the-import-is-read-behind-a-pointer", "readsBehindAPointer", 8),
        ZIG_FLOW_OTHER_READ("the-import-is-read-behind-an-optional", "readsBehindAPointer", 9),
        SL_USE_AT("field-read-has-a-type-read", ZIG_FLOW_FILE, "flow.noFieldRead", "Thing", 8,
                  ZIG_FLOW_FILE, "flow.Thing", "Variable", 2, "const Thing = other.Thing;"),
    };
    static const RQ_Forbidden forbidden[] = {
        ZIG_FLOW_DENY("else-capture-is-the-error", "noElse", "err.go()"),
        ZIG_FLOW_DENY("catch-capture-is-the-error", "noCatchCapture", "bad.go()"),
        ZIG_FLOW_DENY("slice-is-not-its-element", "noSlice", "s.go()"),
        ZIG_FLOW_DENY("for-element-is-not-modelled", "noFor", "i.go()"),
        ZIG_FLOW_DENY("switch-capture-is-not-modelled", "noSwitch", "v.go()"),
        SL_NO_USE_AT("private-through-an-import", ZIG_FLOW_FILE, "flow.noPrivateRead", "SECRET", 0),
        SL_NO_USE_AT("field-has-no-node", ZIG_FLOW_FILE, "flow.noFieldRead", "size", 0),
        SL_NO_USE_AT("anytype-has-no-container", ZIG_FLOW_FILE, "flow.noAnytypeRead", "LIMIT", 1),
        SL_NO_USE_AT("called-member-is-not-also-a-read", ZIG_FLOW_FILE, "flow.callIsNotARead",
                     "helper", 1),
    };
    ASSERT(sl_check("zig-flow", ".zig", "pub fn f() void {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* A type written in a declaration is read in the file of the declaration. */
static const char ZIG_SERVER[] = "const Server = @This();\n"
                                 "const DocumentStore = @import(\"DocumentStore.zig\");\n"
                                 "const Analyser = @import(\"analysis.zig\");\n"
                                 "\n"
                                 "document_store: DocumentStore,\n"
                                 "\n"
                                 "pub fn initAnalyser(server: *Server) Analyser {\n"
                                 "    _ = server;\n"
                                 "    return .{};\n"
                                 "}\n";
static const char ZIG_STORE[] = "const DocumentStore = @This();\n"
                                "pub fn getHandle(self: *DocumentStore) void {\n"
                                "    _ = self;\n"
                                "}\n"
                                "fn hidden(self: *DocumentStore) void {\n"
                                "    _ = self;\n"
                                "}\n";
static const char ZIG_ANALYSIS[] = "const Analyser = @This();\n"
                                   "pub fn getSymbol(analyser: *Analyser) void {\n"
                                   "    _ = analyser;\n"
                                   "}\n";
static const char ZIG_REFERENCES[] =
    "const Server = @import(\"../Server.zig\");\n"
    "pub fn fieldTypedInItsOwnFile(server: *Server) void {\n"
    "    server.document_store.getHandle();\n"
    "}\n"
    "pub fn returnTypedInItsOwnFile(server: *Server) void {\n"
    "    var analyser = server.initAnalyser();\n"
    "    analyser.getSymbol();\n"
    "}\n"
    "pub fn aMethodStillNeedsPub(server: *Server) void {\n"
    "    server.document_store.hidden();\n"
    "}\n"
    "pub fn aConstStillNeedsPubHere(store: *Server.DocumentStore) void {\n"
    "    store.getHandle();\n"
    "}\n";

TEST(resolver_source_zig_a_type_is_read_in_the_file_that_writes_it) {
    static const RQ_Source sources[] = {
        {.path = "src/Server.zig", .text = ZIG_SERVER},
        {.path = "src/DocumentStore.zig", .text = ZIG_STORE},
        {.path = "src/analysis.zig", .text = ZIG_ANALYSIS},
        {.path = "src/features/references.zig", .text = ZIG_REFERENCES},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("a-field-type-written-in-its-own-file", "src/features/references.zig",
                   "references.fieldTypedInItsOwnFile", "server.document_store.getHandle()", 0,
                   "src/DocumentStore.zig", "DocumentStore.getHandle", "Function", 2,
                   "pub fn getHandle(self: *DocumentStore) void {"),
        SL_CALL_AT("a-method-reached-through-a-parameter", "src/features/references.zig",
                   "references.returnTypedInItsOwnFile", "server.initAnalyser()", 0,
                   "src/Server.zig", "Server.initAnalyser", "Function", 7,
                   "pub fn initAnalyser(server: *Server) Analyser {"),
        SL_CALL_AT("a-return-type-written-in-its-own-file", "src/features/references.zig",
                   "references.returnTypedInItsOwnFile", "analyser.getSymbol()", 0,
                   "src/analysis.zig", "analysis.getSymbol", "Function", 2,
                   "pub fn getSymbol(analyser: *Analyser) void {"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("a-method-from-another-file-needs-pub", "src/features/references.zig",
                   "references.aMethodStillNeedsPub", "server.document_store.hidden()", 0),
        SL_DENY_AT("a-const-from-another-file-needs-pub", "src/features/references.zig",
                   "references.aConstStillNeedsPubHere", "store.getHandle()", 0),
    };
    ASSERT(sl_check("zig-written-file", ".zig", "pub fn f() void {}\n", sources,
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
        PL_CALL("class-method-beside-a-rebound-invocant", "no_rebound", "Foo::Base->new",
                "lib/Foo/Base.pm", "Base.new", 3, "sub new {"),
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

static const char RB_BARE[] = "class Bare\n"
                              "  def helper\n"
                              "    1\n"
                              "  end\n"
                              "  def calls\n"
                              "    helper\n"
                              "  end\n"
                              "  def shadowed\n"
                              "    helper = 2\n"
                              "    helper\n"
                              "  end\n"
                              "  def rebound\n"
                              "    helper = 1\n"
                              "    helper = helper + 1\n"
                              "  end\n"
                              "  def param(helper)\n"
                              "    helper\n"
                              "  end\n"
                              "end\n";

#define RB_FILE "lib/sinatra/base.rb"
#define RB_BARE_FILE "lib/sinatra/bare.rb"
#define RB_CALL(id, caller_name, text, tsuffix, tline, tdecl)                                  \
    SL_CALL_AT(id, RB_FILE, caller_name, text, 0, RB_FILE, tsuffix, "Method", tline, tdecl)
#define RB_OWN "    def own"
#define RB_CLASS(id, caller_name, text, at)                                                    \
    SL_CALL_AT(id, RB_FILE, caller_name, text, at, RB_FILE, "Sinatra.Other", "Class", 89,      \
               "  class Other")

#define RB_MENTION(id, caller_name, at)                                                        \
    SL_USE_AT(id, RB_FILE, caller_name, "Other", at, RB_FILE, "Sinatra.Other", "Class", 89,    \
              "  class Other")
TEST(resolver_source_ruby_ancestor_chain_and_self_rules) {
    static const RQ_Source sources[] = {{.path = RB_FILE, .text = RB_BASE},
                                        {.path = RB_BARE_FILE, .text = RB_BARE}};
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
        /* Other declares no initialize, so `new` constructs the class itself. */
        RB_CLASS("new-with-no-initialize-is-the-class", "Base.call_local", "Other.new", 0),
        RB_CLASS("new-of-a-reassigned-local-is-still-the-class", "Base.no_reassigned", "Other.new",
                 1),
        RB_MENTION("constant-of-a-new-is-a-mention", "Base.call_local", 0),
        RB_MENTION("constant-of-a-new-of-a-reassigned-local", "Base.no_reassigned", 1),
        RB_CALL("local-assigned-once-from-new", "Base.call_local", "made.other_m()",
                "Other.other_m", 90, "    def other_m"),
        SL_CALL_AT("block-keeps-self-in-a-method", RB_FILE, "Base.call_in_block", "own()", 2,
                   RB_FILE, "Base.own", "Method", 29, RB_OWN),
        SL_CALL_AT("superclass-method", RB_FILE, "Child.call_inherited", "own()", 5, RB_FILE,
                   "Base.own", "Method", 29, RB_OWN),
        SL_CALL_AT("bare-name-no-assignment-precedes", RB_BARE_FILE, "Bare.calls", "helper", 1,
                   RB_BARE_FILE, "Bare.helper", "Method", 2, "  def helper"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("receiver-of-unknown-class", RB_FILE, "Base.no_unknown_receiver", "thing.own()",
                   0),
        SL_DENY_AT("singleton-method-is-not-on-instances", RB_FILE,
                   "Base.no_singleton_from_instance", "route('GET', '/')", 0),
        SL_DENY_AT("local-assigned-twice", RB_FILE, "Base.no_reassigned", "made.other_m()", 1),
        SL_DENY_AT("constant-outside-the-project", RB_FILE, "Base.no_outside", "Outside.settings",
                   0),
        SL_DENY_AT("bare-name-after-its-assignment-is-the-local", RB_BARE_FILE, "Bare.shadowed",
                   "helper", 3),
        SL_DENY_AT("right-of-a-second-assignment-is-the-local", RB_BARE_FILE, "Bare.rebound",
                   "helper", 6),
        SL_DENY_AT("parameter-is-the-local", RB_BARE_FILE, "Bare.param", "helper", 8),
    };
    ASSERT(sl_check("ruby-rules", ".rb", "def filler\n  1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

static const char RB2_MAIN[] =
    "module App\n"
    "  module PathUtil\n"
    "    def before_function\n"
    "      0\n"
    "    end\n"
    "\n"
    "    module_function\n"
    "\n"
    "    def smart_path(path)\n"
    "      path\n"
    "    end\n"
    "  end\n"
    "\n"
    "  module Explicit\n"
    "    def listed\n"
    "      1\n"
    "    end\n"
    "    module_function :listed\n"
    "  end\n"
    "\n"
    "  class Corrector < ::Parser::Source::TreeRewriter\n"
    "    def initialize(source)\n"
    "      @source = source\n"
    "    end\n"
    "\n"
    "    def fix\n"
    "      2\n"
    "    end\n"
    "  end\n"
    "\n"
    "  class NoInit\n"
    "    def work\n"
    "      3\n"
    "    end\n"
    "  end\n"
    "\n"
    "  class Factory\n"
    "    def self.new\n"
    "      Object.new\n"
    "    end\n"
    "\n"
    "    def work\n"
    "      4\n"
    "    end\n"
    "  end\n"
    "\n"
    "  class Parent\n"
    "    PARENT_LIMIT = 9\n"
    "\n"
    "    class Inner\n"
    "      def inner_m\n"
    "        5\n"
    "      end\n"
    "    end\n"
    "  end\n"
    "\n"
    "  class Cop < Parent\n"
    "    MSG = 'x'\n"
    "    DEFAULT = NoInit\n"
    "\n"
    "    class << self\n"
    "      attr_accessor :debug\n"
    "\n"
    "      def registry\n"
    "        6\n"
    "      end\n"
    "\n"
    "      def registry=(value)\n"
    "        @registry = value\n"
    "      end\n"
    "    end\n"
    "\n"
    "    attr_accessor :name\n"
    "\n"
    "    def name\n"
    "      @name\n"
    "    end\n"
    "\n"
    "    def resource=(value)\n"
    "      @resource = value\n"
    "    end\n"
    "\n"
    "    def resource\n"
    "      @resource\n"
    "    end\n"
    "\n"
    "    def reader_only\n"
    "      7\n"
    "    end\n"
    "\n"
    "    attr_accessor :accessor_only\n"
    "\n"
    "    private def hidden\n"
    "      8\n"
    "    end\n"
    "\n"
    "    def old_m\n"
    "      9\n"
    "    end\n"
    "    alias_method :new_m, :old_m\n"
    "    alias also_m old_m\n"
    "\n"
    "    alias_method :orig_wrapped, :wrapped\n"
    "    def wrapped\n"
    "      orig_wrapped()\n"
    "    end\n"
    "\n"
    "    def helper_obj\n"
    "      10\n"
    "    end\n"
    "\n"
    "    def calls_module_function\n"
    "      PathUtil.smart_path(1)\n"
    "    end\n"
    "\n"
    "    def no_def_before_module_function\n"
    "      PathUtil.before_function\n"
    "    end\n"
    "\n"
    "    def calls_listed_function\n"
    "      Explicit.listed\n"
    "    end\n"
    "\n"
    "    def calls_private_def\n"
    "      hidden()\n"
    "    end\n"
    "\n"
    "    def calls_alias_method\n"
    "      new_m()\n"
    "    end\n"
    "\n"
    "    def calls_alias_keyword\n"
    "      also_m()\n"
    "    end\n"
    "\n"
    "    def calls_later_def_over_attr\n"
    "      name()\n"
    "    end\n"
    "\n"
    "    def calls_new_outside_parent\n"
    "      Corrector.new(1)\n"
    "    end\n"
    "\n"
    "    def calls_new_chain\n"
    "      Corrector.new(2).fix\n"
    "    end\n"
    "\n"
    "    def calls_new_no_initialize\n"
    "      NoInit.new\n"
    "    end\n"
    "\n"
    "    def calls_new_through_constant_alias\n"
    "      DEFAULT.new\n"
    "    end\n"
    "\n"
    "    def calls_writer\n"
    "      self.resource = 1\n"
    "    end\n"
    "\n"
    "    def calls_reader_and_writer\n"
    "      self.resource ||= 1\n"
    "    end\n"
    "\n"
    "    def calls_class_chain\n"
    "      self.class.registry\n"
    "    end\n"
    "\n"
    "    def calls_constant_writer\n"
    "      Cop.registry = 1\n"
    "    end\n"
    "\n"
    "    def receiver_of_attribute_write(value)\n"
    "      helper_obj.thing = value\n"
    "    end\n"
    "\n"
    "    def no_writer_is_not_its_reader\n"
    "      self.reader_only = 1\n"
    "    end\n"
    "\n"
    "    def no_accessor_node\n"
    "      self.accessor_only = 1\n"
    "    end\n"
    "\n"
    "    def no_custom_new\n"
    "      Factory.new.work\n"
    "    end\n"
    "\n"
    "    def no_value_constant\n"
    "      MSG.size\n"
    "    end\n"
    "\n"
    "    def inherited_constant\n"
    "      Inner.new.inner_m\n"
    "    end\n"
    "  end\n"
    "end\n"
    "\n"
    "class App::Compact\n"
    "  def lexical_scope_skips_app\n"
    "    Lexical.pick\n"
    "  end\n"
    "end\n"
    "\n"
    "class Lexical\n"
    "  def self.pick\n"
    "    11\n"
    "  end\n"
    "end\n"
    "\n"
    "module App\n"
    "  class Lexical\n"
    "    def self.pick\n"
    "      12\n"
    "    end\n"
    "  end\n"
    "end\n";

static const char RB3_BLOCKS[] =
    "module App\n"
    "  class Worker\n"
    "    def add_offense(node, &block)\n"
    "      correct(node, &block)\n"
    "    end\n"
    "\n"
    "    def correct(node)\n"
    "      yield node\n"
    "    end\n"
    "\n"
    "    def configure(&block)\n"
    "      instance_eval(&block)\n"
    "    end\n"
    "\n"
    "    def relay(&block)\n"
    "      configure(&block)\n"
    "    end\n"
    "\n"
    "    def keep(&block)\n"
    "      @kept = block\n"
    "    end\n"
    "\n"
    "    def splice(&block)\n"
    "      lots.map(&block)\n"
    "    end\n"
    "\n"
    "    def helper\n"
    "      1\n"
    "    end\n"
    "\n"
    "    def self.class_helper\n"
    "      2\n"
    "    end\n"
    "\n"
    "    def in_core_iterator(items)\n"
    "      items.each { |item| helper() }\n"
    "    end\n"
    "\n"
    "    def in_core_chain(items)\n"
    "      items.map { |item| item }.select { |item| helper() }\n"
    "    end\n"
    "\n"
    "    def in_project_method\n"
    "      add_offense(1) { |x| helper() }\n"
    "    end\n"
    "\n"
    "    def in_forward_to_core\n"
    "      splice { |x| helper() }\n"
    "    end\n"
    "\n"
    "    def no_instance_eval\n"
    "      configure { helper() }\n"
    "    end\n"
    "\n"
    "    def no_forward_to_instance_eval\n"
    "      relay { helper() }\n"
    "    end\n"
    "\n"
    "    def no_kept_block\n"
    "      keep { helper() }\n"
    "    end\n"
    "\n"
    "    def no_unknown_method(thing)\n"
    "      thing.frobnicate { helper() }\n"
    "    end\n"
    "\n"
    "    %w[a].each { |name| class_helper() }\n"
    "\n"
    "    define_method(:made) { helper() }\n"
    "\n"
    "    scope :recent, -> { class_helper() }\n"
    "\n"
    "    get '/' do\n"
    "      class_helper()\n"
    "    end\n"
    "  end\n"
    "end\n";

static const char RB4_OWN_EACH[] =
    "class Dsl\n"
    "  def each(&block)\n"
    "    instance_eval(&block)\n"
    "  end\n"
    "end\n"
    "\n"
    "class Uses\n"
    "  def helper\n"
    "    1\n"
    "  end\n"
    "\n"
    "  def no_project_each(items)\n"
    "    items.each { |item| helper() }\n"
    "  end\n"
    "\n"
    "  def map_is_still_core(items)\n"
    "    items.map { |item| helper() }\n"
    "  end\n"
    "end\n";

static const char RB5_REFS[] =
    "require_relative 'other'\n"
    "\n"
    "limit = 1\n"
    "puts limit\n"
    "\n"
    "module App\n"
    "  class Thing\n"
    "    KIND = 'k'\n"
    "\n"
    "    def mentions_class\n"
    "      Solo\n"
    "    end\n"
    "\n"
    "    def mentions_path\n"
    "      Inner::Deep\n"
    "    end\n"
    "\n"
    "    def mentions_value\n"
    "      KIND\n"
    "    end\n"
    "\n"
    "    def no_local\n"
    "      limit = 2\n"
    "      limit\n"
    "    end\n"
    "\n"
    "    def no_reopened_namespace\n"
    "      Spread\n"
    "    end\n"
    "\n"
    "    def no_outside\n"
    "      Gemfile::Thing\n"
    "    end\n"
    "\n"
    "    def mentions_in_rescue\n"
    "      1\n"
    "    rescue Solo\n"
    "      2\n"
    "    end\n"
    "  end\n"
    "\n"
    "  class Solo\n"
    "  end\n"
    "\n"
    "  module Inner\n"
    "    class Deep\n"
    "    end\n"
    "  end\n"
    "\n"
    "  class Spread\n"
    "  end\n"
    "end\n";

static const char RB5_OTHER[] =
    "module App\n"
    "  class Spread\n"
    "  end\n"
    "end\n";

static const char RB6_INDEX[] =
    "class Store\n"
    "  def initialize(a = 1)\n"
    "    @a = a\n"
    "  end\n"
    "\n"
    "  def [](key)\n"
    "    key\n"
    "  end\n"
    "\n"
    "  def []=(key, value)\n"
    "    value\n"
    "  end\n"
    "\n"
    "  def self.build(a)\n"
    "    new(a)\n"
    "  end\n"
    "\n"
    "  def self.memo(a)\n"
    "    @memo ||= new(a)\n"
    "  end\n"
    "\n"
    "  def no_instance_new(a)\n"
    "    new(a)\n"
    "  end\n"
    "\n"
    "  def reads_self(key)\n"
    "    self[key]\n"
    "  end\n"
    "\n"
    "  def writes_self(key)\n"
    "    self[key] = 1\n"
    "  end\n"
    "\n"
    "  def both_self(key)\n"
    "    self[key] ||= 1\n"
    "  end\n"
    "\n"
    "  def reads_typed\n"
    "    other = Store.new\n"
    "    other[1]\n"
    "  end\n"
    "\n"
    "  def no_unknown_receiver(thing)\n"
    "    thing[1]\n"
    "  end\n"
    "\n"
    "  def no_array_literal\n"
    "    [1][0]\n"
    "  end\n"
    "end\n"
    "\n"
    "class Plain\n"
    "  def no_index_on_a_class_without_it\n"
    "    self[1]\n"
    "  end\n"
    "end\n";

static const char RB7_CORRECTOR[] = "module Helper\n"
                                    "  def from_helper\n"
                                    "    1\n"
                                    "  end\n"
                                    "end\n"
                                    "\n"
                                    "class Corrector\n"
                                    "  class << self\n"
                                    "    include Helper\n"
                                    "\n"
                                    "    def correct\n"
                                    "      from_helper()\n"
                                    "    end\n"
                                    "  end\n"
                                    "\n"
                                    "  def no_instance_reach\n"
                                    "    from_helper()\n"
                                    "  end\n"
                                    "end\n";

static const char RB8_SUPER[] = "module Mixed\n"
                                "  def through_mixin\n"
                                "    1\n"
                                "  end\n"
                                "end\n"
                                "\n"
                                "class Parent\n"
                                "  def go(a)\n"
                                "    a\n"
                                "  end\n"
                                "\n"
                                "  def self.make\n"
                                "    1\n"
                                "  end\n"
                                "\n"
                                "  def through_mixin\n"
                                "    2\n"
                                "  end\n"
                                "end\n"
                                "\n"
                                "class Child < Parent\n"
                                "  include Mixed\n"
                                "\n"
                                "  def go(a)\n"
                                "    super\n"
                                "  end\n"
                                "\n"
                                "  def through_mixin\n"
                                "    super()\n"
                                "  end\n"
                                "\n"
                                "  def self.make\n"
                                "    super\n"
                                "  end\n"
                                "\n"
                                "  def no_parent_method\n"
                                "    super\n"
                                "  end\n"
                                "end\n"
                                "\n"
                                "class Outsider < Gem::Thing\n"
                                "  def no_chain_out\n"
                                "    super\n"
                                "  end\n"
                                "end\n"
                                "\n"
                                "module Mod\n"
                                "  def no_module_knows\n"
                                "    super\n"
                                "  end\n"
                                "end\n";

#define RB2_FILE "lib/app/cop.rb"
#define RB6_FILE "lib/app/store.rb"
#define RB6_CALL(id, caller, text, at, tsuffix, line, decl)                                    \
    SL_CALL_AT(id, RB6_FILE, "Store." caller, text, at, RB6_FILE, "Store." tsuffix, "Method",  \
               line, decl)
#define RB3_FILE "lib/app/workers.rb"
#define RB4_FILE "lib/app/uses.rb"
#define RB5_FILE "lib/app/refs.rb"
#define SL_WRITE_AT(id, sfile, scaller, text, at, tfile, tsuffix, tlabel, tline, tdecl)        \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "WRITES",                                                                  \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = tlabel,                                                            \
                   .declaration_line = tline,                                                  \
                   .declaration_text = tdecl},                                                 \
    }

/* Forms from devise, vagrant and rubocop. */
TEST(resolver_source_ruby_writers_constructors_aliases_and_constants) {
    static const RQ_Source sources[] = {{.path = RB2_FILE, .text = RB2_MAIN}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("module-function-copies-onto-the-module", RB2_FILE, "Cop.calls_module_function", "PathUtil.smart_path(1)", 0, RB2_FILE, "PathUtil.smart_path", "Method", 9,
                   "    def smart_path(path)"),
        SL_CALL_AT("module-function-with-a-literal-name", RB2_FILE, "Cop.calls_listed_function", "Explicit.listed", 0, RB2_FILE, "Explicit.listed", "Method", 15,
                   "    def listed"),
        SL_CALL_AT("private-def-is-declared", RB2_FILE, "Cop.calls_private_def", "hidden()", 0, RB2_FILE, "Cop.hidden", "Method", 93,
                   "    private def hidden"),
        SL_CALL_AT("alias-method-names-the-earlier-def", RB2_FILE, "Cop.calls_alias_method", "new_m()", 0, RB2_FILE, "Cop.old_m", "Method", 97,
                   "    def old_m"),
        SL_CALL_AT("alias-keyword-names-the-earlier-def", RB2_FILE, "Cop.calls_alias_keyword", "also_m()", 0, RB2_FILE, "Cop.old_m", "Method", 97,
                   "    def old_m"),
        SL_CALL_AT("a-later-def-replaces-an-attribute-macro", RB2_FILE, "Cop.calls_later_def_over_attr", "name()", 0, RB2_FILE, "Cop.name", "Method", 75,
                   "    def name"),
        SL_CALL_AT("new-selects-initialize-with-an-outside-parent", RB2_FILE, "Cop.calls_new_outside_parent", "Corrector.new(1)", 0, RB2_FILE, "Corrector.initialize", "Method", 22,
                   "    def initialize(source)"),
        SL_CALL_AT("new-gives-the-chain-its-class-initialize", RB2_FILE, "Cop.calls_new_chain", "Corrector.new(2)", 0, RB2_FILE, "Corrector.initialize", "Method", 22,
                   "    def initialize(source)"),
        SL_CALL_AT("new-gives-the-chain-its-class-method", RB2_FILE, "Cop.calls_new_chain", "Corrector.new(2).fix", 0, RB2_FILE, "Corrector.fix", "Method", 26,
                   "    def fix"),
        SL_CALL_AT("new-with-no-initialize-is-the-class", RB2_FILE, "Cop.calls_new_no_initialize", "NoInit.new", 0, RB2_FILE, "App.NoInit", "Class", 31,
                   "  class NoInit"),
        SL_USE_AT("constant-receiver-of-new-is-a-mention", RB2_FILE, "Cop.calls_new_no_initialize",
                  "NoInit", 2, RB2_FILE, "App.NoInit", "Class", 31, "  class NoInit"),
        SL_USE_AT("inherited-constant-is-a-mention", RB2_FILE, "Cop.inherited_constant", "Inner", 1,
                  RB2_FILE, "Parent.Inner", "Class", 50, "    class Inner"),
        SL_CALL_AT("constant-holding-a-class-reads-as-it", RB2_FILE, "Cop.calls_new_through_constant_alias", "DEFAULT.new", 0, RB2_FILE, "App.NoInit", "Class", 31,
                   "  class NoInit"),
        SL_CALL_AT("custom-new-is-what-new-selects", RB2_FILE, "Cop.no_custom_new", "Factory.new", 0, RB2_FILE, "Factory.new", "Method", 38,
                   "    def self.new"),
        SL_CALL_AT("attribute-write-calls-the-writer", RB2_FILE, "Cop.calls_writer", "self.resource", 0, RB2_FILE, "Cop.resource=", "Method", 79,
                   "    def resource=(value)"),
        SL_CALL_AT("operator-write-reads-through-the-reader", RB2_FILE, "Cop.calls_reader_and_writer", "self.resource", 1, RB2_FILE, "Cop.resource", "Method", 83,
                   "    def resource"),
        SL_CALL_AT("operator-write-then-calls-the-writer", RB2_FILE, "Cop.calls_reader_and_writer", "resource", 5, RB2_FILE, "Cop.resource=", "Method", 79,
                   "    def resource=(value)"),
        SL_CALL_AT("self-class-searches-the-singleton-chain", RB2_FILE, "Cop.calls_class_chain", "self.class.registry", 0, RB2_FILE, "Cop.registry", "Method", 64,
                   "      def registry"),
        SL_CALL_AT("constant-attribute-write-calls-the-singleton-writer", RB2_FILE, "Cop.calls_constant_writer", "Cop.registry", 0, RB2_FILE, "Cop.registry=", "Method", 68,
                   "      def registry=(value)"),
        SL_CALL_AT("receiver-of-an-attribute-write-stays-a-call", RB2_FILE, "Cop.receiver_of_attribute_write", "helper_obj", 1, RB2_FILE, "Cop.helper_obj", "Method", 108,
                   "    def helper_obj"),
        SL_CALL_AT("constant-inherited-from-a-superclass", RB2_FILE, "Cop.inherited_constant", "Inner.new", 0, RB2_FILE, "Parent.Inner", "Class", 50,
                   "    class Inner"),
        SL_CALL_AT("class-of-an-inherited-constant", RB2_FILE, "Cop.inherited_constant", "Inner.new.inner_m", 0, RB2_FILE, "Inner.inner_m", "Method", 51,
                   "      def inner_m"),
        SL_CALL_AT("compact-class-header-skips-the-enclosing-module", RB2_FILE, "App::Compact.lexical_scope_skips_app", "Lexical.pick", 0, RB2_FILE, "Lexical.pick", "Method", 205,
                   "  def self.pick"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("alias-of-a-later-def-is-what-it-replaced", RB2_FILE, "Cop.wrapped", "orig_wrapped()", 0),
        SL_DENY_AT("def-before-module-function-is-not-copied", RB2_FILE, "Cop.no_def_before_module_function", "PathUtil.before_function", 0),
        SL_DENY_AT("writer-is-not-its-reader", RB2_FILE, "Cop.no_writer_is_not_its_reader", "self.reader_only", 0),
        SL_DENY_AT("attribute-macro-has-no-node", RB2_FILE, "Cop.no_accessor_node", "self.accessor_only", 0),
        SL_DENY_AT("custom-new-returns-anything", RB2_FILE, "Cop.no_custom_new", "Factory.new.work", 0),
        SL_DENY_AT("constant-holding-a-value-has-no-class", RB2_FILE, "Cop.no_value_constant", "MSG.size", 0),
    };
    ASSERT(sl_check("ruby-members", ".rb", "def filler\n  1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

TEST(resolver_source_ruby_block_keeps_self_only_for_a_method_that_lets_it) {
    static const RQ_Source sources[] = {{.path = RB3_FILE, .text = RB3_BLOCKS}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("project-call-with-a-block", RB3_FILE, "Worker.in_project_method",
                   "add_offense(1) { |x| helper() }", 0, RB3_FILE, "Worker.add_offense", "Method", 3,
                   "    def add_offense(node, &block)"),
        SL_CALL_AT("project-call-forwarding-to-core", RB3_FILE, "Worker.in_forward_to_core",
                   "splice { |x| helper() }", 0, RB3_FILE, "Worker.splice", "Method", 23,
                   "    def splice(&block)"),
        SL_CALL_AT("project-call-that-rebinds-is-still-a-call", RB3_FILE, "Worker.no_instance_eval",
                   "configure { helper() }", 0, RB3_FILE, "Worker.configure", "Method", 11,
                   "    def configure(&block)"),
        SL_CALL_AT("project-call-that-forwards-is-still-a-call", RB3_FILE,
                   "Worker.no_forward_to_instance_eval", "relay { helper() }", 0, RB3_FILE,
                   "Worker.relay", "Method", 15, "    def relay(&block)"),
        SL_CALL_AT("project-call-that-keeps-is-still-a-call", RB3_FILE, "Worker.no_kept_block",
                   "keep { helper() }", 0, RB3_FILE, "Worker.keep", "Method", 19,
                   "    def keep(&block)"),
        SL_CALL_AT("core-iterator-keeps-self", RB3_FILE, "Worker.in_core_iterator", "helper()", 0, RB3_FILE, "Worker.helper", "Method", 27,
                   "    def helper"),
        SL_CALL_AT("core-iterators-in-a-chain-keep-self", RB3_FILE, "Worker.in_core_chain", "helper()", 1, RB3_FILE, "Worker.helper", "Method", 27,
                   "    def helper"),
        SL_CALL_AT("project-method-that-only-yields-keeps-self", RB3_FILE, "Worker.in_project_method", "helper()", 2, RB3_FILE, "Worker.helper", "Method", 27,
                   "    def helper"),
        SL_CALL_AT("project-method-forwarding-to-a-core-iterator", RB3_FILE, "Worker.in_forward_to_core", "helper()", 3, RB3_FILE, "Worker.helper", "Method", 27,
                   "    def helper"),
        SL_CALL_AT("core-iterator-in-a-class-body-keeps-the-class", RB3_FILE, "workers", "class_helper()", 0, RB3_FILE, "Worker.class_helper", "Method", 31,
                   "    def self.class_helper"),
        SL_CALL_AT("define-method-block-is-an-instance-method-body", RB3_FILE, "workers", "helper()", 9, RB3_FILE, "Worker.helper", "Method", 27,
                   "    def helper"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("instance-eval-in-the-project-def", RB3_FILE, "Worker.no_instance_eval", "helper()", 4),
        SL_DENY_AT("forwarded-into-instance-eval", RB3_FILE, "Worker.no_forward_to_instance_eval", "helper()", 5),
        SL_DENY_AT("project-def-keeps-the-block", RB3_FILE, "Worker.no_kept_block", "helper()", 6),
        SL_DENY_AT("unknown-method-can-rebind", RB3_FILE, "Worker.no_unknown_method", "helper()", 7),
        SL_DENY_AT("lambda-in-a-class-body-is-handed-over", RB3_FILE, "workers", "class_helper()", 1),
        SL_DENY_AT("dsl-block-in-a-class-body", RB3_FILE, "workers", "class_helper()", 2),
    };
    ASSERT(sl_check("ruby-blocks", ".rb", "def filler\n  1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* The project defines each with instance_eval, so no `.each` receiver can be trusted. */
TEST(resolver_source_ruby_project_each_that_rebinds_blocks_the_core_rule) {
    static const RQ_Source sources[] = {{.path = RB4_FILE, .text = RB4_OWN_EACH}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("other-core-iterators-still-keep-self", RB4_FILE, "Uses.map_is_still_core", "helper()", 1, RB4_FILE, "Uses.helper", "Method", 8,
                   "  def helper"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("each-is-the-projects-own", RB4_FILE, "Uses.no_project_each", "helper()", 0),
    };
    ASSERT(sl_check("ruby-own-each", ".rb", "def filler\n  1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

TEST(resolver_source_ruby_constants_and_file_locals_are_references) {
    static const RQ_Source sources[] = {{.path = RB5_FILE, .text = RB5_REFS},
                                        {.path = "lib/app/other.rb", .text = RB5_OTHER}};
    static const RQ_Expected expected[] = {
        SL_USE_AT("class-mention", RB5_FILE, "Thing.mentions_class", "Solo", 0, RB5_FILE, "App.Solo", "Class", 42,
                   "  class Solo"),
        SL_USE_AT("whole-path-mention", RB5_FILE, "Thing.mentions_path", "Inner::Deep", 0, RB5_FILE, "Inner.Deep", "Class", 46,
                   "    class Deep"),
        SL_USE_AT("path-prefix-is-read-on-the-way", RB5_FILE, "Thing.mentions_path", "Inner", 0, RB5_FILE, "App.Inner", "Class", 45,
                   "  module Inner"),
        SL_USE_AT("value-constant-mention", RB5_FILE, "Thing.mentions_value", "KIND", 1, RB5_FILE, "refs.KIND", "Variable", 8,
                   "    KIND = 'k'"),
        SL_USE_AT("rescue-names-a-class", RB5_FILE, "Thing.mentions_in_rescue", "Solo", 1, RB5_FILE, "App.Solo", "Class", 42,
                   "  class Solo"),
        SL_USE_AT("file-level-local-is-read", RB5_FILE, "refs", "limit", 1, RB5_FILE, "refs.limit", "Variable", 3,
                   "limit = 1"),
        SL_WRITE_AT("file-level-local-is-written", RB5_FILE, "refs", "limit", 0, RB5_FILE, "refs.limit", "Variable", 3,
                   "limit = 1"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("local-in-a-def-has-no-node", RB5_FILE, "Thing.no_local", "limit", 3),
        SL_NO_USE_AT("reopened-namespace-is-no-single-declaration", RB5_FILE, "Thing.no_reopened_namespace", "Spread", 0),
        SL_NO_USE_AT("constant-outside-the-project", RB5_FILE, "Thing.no_outside", "Gemfile::Thing", 0),
    };
    ASSERT(sl_check("ruby-references", ".rb", "def filler\n  1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

#define SL_REF_AT(id, sfile, scaller, text, at, tfile, tsuffix, tlabel, tline, tdecl)          \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "CALL_REFERENCE",                                                          \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = tlabel,                                                            \
                   .declaration_line = tline,                                                  \
                   .declaration_text = tdecl},                                                 \
    }
#define SL_NO_REF_AT(id, sfile, scaller, text, at)                                             \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "CALL_REFERENCE", .any_target = true                                       \
    }

/* ── Elixir: references and what the first round left out ──────────── */

static const char EX2_PROTOCOL[] =
    "defprotocol Plug.Exception do\n"
    "  def status(exception)\n"
    "end\n";
static const char EX2_IMPL[] =
    "defimpl Plug.Exception, for: Any do\n"
    "  def status(_exception), do: fallback()\n"
    "  defp fallback, do: 500\n"
    "end\n";
static const char EX2_OTHER[] =
    "defmodule Plug.Other do\n"
    "  def pub(x), do: x\n"
    "  defp hid(x), do: x\n"
    "end\n";
static const char EX2_ERL[] =
    "-module(plug_multipart).\n"
    "-export([parse_headers/2]).\n"
    "parse_headers(A, B) -> {A, B}.\n"
    "hidden(A) -> A.\n";
static const char EX2_REFS[] =
    "defmodule Plug.Refs do\n"
    "  alias Plug.Other, as: O\n"
    "  def fetch_it(conn), do: conn\n"
    "  def chained(conn), do: fetch_it(conn).params\n"
    "  def proto(e), do: Plug.Exception.status(e)\n"
    "  def erl(data), do: :plug_multipart.parse_headers(data, 1)\n"
    "  def no_erl_private(data), do: :plug_multipart.hidden(data)\n"
    "  def no_erl_outside(data), do: :binary.match(data, 1)\n"
    "  def piped(conn), do: conn |> fetch_it\n"
    "  def no_variable(fetch_it), do: fetch_it\n"
    "  def local_capture, do: run(&fetch_it/1)\n"
    "  def remote_capture, do: run(&Plug.Other.pub/1)\n"
    "  def alias_capture, do: run(&O.pub/1)\n"
    "  def no_private_capture, do: run(&Plug.Other.hid/1)\n"
    "  def no_arity_capture, do: run(&fetch_it/2)\n"
    "  def mentions, do: Plug.Other\n"
    "  def mentions_alias, do: O\n"
    "  def no_outside_module, do: Some.Outside\n"
    "  defp run(fun), do: fun\n"
    "end\n";

#define EX2_FILE "lib/plug/refs.ex"
#define EX2_ERL_FILE "src/plug_multipart.erl"

TEST(resolver_source_elixir_captures_mentions_protocols_and_erlang_atoms) {
    static const RQ_Source sources[] = {
        {.path = "lib/plug/exception.ex", .text = EX2_PROTOCOL},
        {.path = "lib/plug/exception_any.ex", .text = EX2_IMPL},
        {.path = "lib/plug/other.ex", .text = EX2_OTHER},
        {.path = EX2_ERL_FILE, .text = EX2_ERL},
        {.path = EX2_FILE, .text = EX2_REFS},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("receiver-of-a-field-is-read-first", EX2_FILE, "refs.chained", "fetch_it(conn)", 1, EX2_FILE, "refs.fetch_it", "Function", 3,
                   "  def fetch_it(conn), do: conn"),
        SL_CALL_AT("protocol-function-is-what-the-call-names", EX2_FILE, "refs.proto", "Plug.Exception.status(e)", 0, "lib/plug/exception.ex", "exception.status", "Function", 2,
                   "  def status(exception)"),
        SL_CALL_AT("impl-calls-its-own-function", "lib/plug/exception_any.ex", "exception_any.status", "fallback()", 0, "lib/plug/exception_any.ex", "exception_any.fallback", "Function", 3,
                   "  defp fallback, do: 500"),
        SL_CALL_AT("atom-receiver-is-an-erlang-module", EX2_FILE, "refs.erl", ":plug_multipart.parse_headers(data, 1)", 0, EX2_ERL_FILE, "plug_multipart.parse_headers", "Function", 3,
                   "parse_headers(A, B) -> {A, B}."),
        SL_CALL_AT("pipe-into-a-bare-name-calls-it", EX2_FILE, "refs.piped", "fetch_it", 2, EX2_FILE, "refs.fetch_it", "Function", 3,
                   "  def fetch_it(conn), do: conn"),
        SL_REF_AT("local-capture", EX2_FILE, "refs.local_capture", "&fetch_it/1", 0, EX2_FILE, "refs.fetch_it", "Function", 3,
                   "  def fetch_it(conn), do: conn"),
        SL_REF_AT("remote-capture", EX2_FILE, "refs.remote_capture", "&Plug.Other.pub/1", 0, "lib/plug/other.ex", "other.pub", "Function", 2,
                   "  def pub(x), do: x"),
        SL_REF_AT("capture-through-an-alias", EX2_FILE, "refs.alias_capture", "&O.pub/1", 0, "lib/plug/other.ex", "other.pub", "Function", 2,
                   "  def pub(x), do: x"),
        SL_USE_AT("module-name-is-a-mention", EX2_FILE, "refs.mentions", "Plug.Other", 3, "lib/plug/other.ex", "Plug.Other", "Class", 1,
                   "defmodule Plug.Other do"),
        SL_USE_AT("aliased-module-name-is-a-mention", EX2_FILE, "refs.mentions_alias", "O", 6, "lib/plug/other.ex", "Plug.Other", "Class", 1,
                   "defmodule Plug.Other do"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("erlang-function-not-exported", EX2_FILE, "refs.no_erl_private", ":plug_multipart.hidden(data)", 0),
        SL_DENY_AT("erlang-module-outside-the-project", EX2_FILE, "refs.no_erl_outside", ":binary.match(data, 1)", 0),
        SL_DENY_AT("bare-name-is-a-variable", EX2_FILE, "refs.no_variable", "fetch_it", 4),
        SL_NO_REF_AT("private-function-does-not-capture", EX2_FILE, "refs.no_private_capture", "&Plug.Other.hid/1", 0),
        SL_NO_REF_AT("capture-of-an-absent-arity", EX2_FILE, "refs.no_arity_capture", "&fetch_it/2", 0),
        SL_NO_USE_AT("module-outside-the-project", EX2_FILE, "refs.no_outside_module", "Some.Outside", 0),
    };
    ASSERT(sl_check("elixir-references", ".ex", "defmodule Filler do\n  def f, do: 1\nend\n",
                    sources, sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Elixir: what `use` is known to bring ──────────────────────────── */

static const char EX3_CONN[] = "defmodule My.Conn do\n"
                               "  def assign(conn, key), do: {conn, key}\n"
                               "  def halt(conn), do: conn\n"
                               "end\n";
static const char EX3_BUILDER[] =
    "defmodule My.Builder do\n"
    "  defmacro __using__(_opts) do\n"
    "    quote do\n"
    "      import My.Conn, except: [halt: 1]\n"
    "      import My.Builder, only: [plug: 1]\n"
    "      def call(conn, opts), do: {conn, opts}\n"
    "    end\n"
    "  end\n"
    "  defmacro plug(name), do: name\n"
    "  defmacro not_listed(name), do: name\n"
    "  def call(conn), do: conn\n"
    "end\n";
static const char EX3_ROUTER[] = "defmodule My.Router do\n"
                                 "  defmacro __using__(opts) do\n"
                                 "    quote location: :keep do\n"
                                 "      import unquote(__MODULE__)\n"
                                 "      use My.Builder, unquote(opts)\n"
                                 "    end\n"
                                 "  end\n"
                                 "  defmacro get(path, contents), do: {path, contents}\n"
                                 "end\n";
static const char EX3_DISCARDS[] = "defmodule My.Discards do\n"
                                   "  defmacro __using__(_opts) do\n"
                                   "    _ = quote do\n"
                                   "      import My.Conn\n"
                                   "    end\n"
                                   "    :ok\n"
                                   "  end\n"
                                   "end\n";
static const char EX3_APP[] =
    "defmodule My.App do\n"
    "  use My.Router\n"
    "  plug :first\n"
    "  get \"/\" do\n"
    "    :ok\n"
    "  end\n"
    "  def through_two_uses(conn), do: assign(conn, 1)\n"
    "  def no_except(conn), do: halt(conn)\n"
    "  def no_only(name), do: not_listed(name)\n"
    "  def no_injected(conn, opts), do: call(conn, opts)\n"
    "end\n";
static const char EX3_PLAIN[] = "defmodule My.Plain do\n"
                                "  def no_use(conn), do: assign(conn, 1)\n"
                                "end\n";
static const char EX3_USES_DISCARDS[] = "defmodule My.UsesDiscards do\n"
                                        "  use My.Discards\n"
                                        "  def no_discarded(conn), do: assign(conn, 1)\n"
                                        "end\n";
static const char EX3_SCOPED[] = "defmodule My.Scoped do\n"
                                 "  def in_block(conn) do\n"
                                 "    if conn do\n"
                                 "      use My.Builder\n"
                                 "      assign(conn, 1)\n"
                                 "    end\n"
                                 "    assign(conn, 2)\n"
                                 "  end\n"
                                 "end\n";

static const char EX3_OUTER[] = "defmodule My.Outer do\n"
                                "  import My.Conn\n"
                                "  defmodule Nested do\n"
                                "    use My.Router\n"
                                "    def in_nested(conn), do: assign(conn, 3)\n"
                                "    def halts_in_nested(conn), do: halt(conn)\n"
                                "  end\n"
                                "  def after_nested(conn), do: halt(conn)\n"
                                "  def no_macro_after_nested(name), do: plug(name)\n"
                                "end\n";

TEST(resolver_source_elixir_use_brings_what_using_returns) {
    static const RQ_Source sources[] = {
        {.path = "lib/my/conn.ex", .text = EX3_CONN},
        {.path = "lib/my/builder.ex", .text = EX3_BUILDER},
        {.path = "lib/my/router.ex", .text = EX3_ROUTER},
        {.path = "lib/my/discards.ex", .text = EX3_DISCARDS},
        {.path = "lib/my/app.ex", .text = EX3_APP},
        {.path = "lib/my/plain.ex", .text = EX3_PLAIN},
        {.path = "lib/my/uses_discards.ex", .text = EX3_USES_DISCARDS},
        {.path = "lib/my/scoped.ex", .text = EX3_SCOPED},
        {.path = "lib/my/outer.ex", .text = EX3_OUTER},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("import-of-the-macro-module-itself", "lib/my/app.ex", "my.app",
                   "get \"/\" do\n    :ok\n  end", 0, "lib/my/router.ex", "router.get", "Function",
                   8, "  defmacro get(path, contents), do: {path, contents}"),
        SL_CALL_AT("import-with-only-through-a-second-use", "lib/my/app.ex", "my.app",
                   "plug :first", 0, "lib/my/builder.ex", "builder.plug", "Function", 9,
                   "  defmacro plug(name), do: name"),
        SL_CALL_AT("import-through-a-second-use", "lib/my/app.ex", "app.through_two_uses",
                   "assign(conn, 1)", 0, "lib/my/conn.ex", "conn.assign", "Function", 2,
                   "  def assign(conn, key), do: {conn, key}"),
        SL_CALL_AT("nested-module-keeps-the-imports-around-it", "lib/my/outer.ex",
                   "outer.halts_in_nested", "halt(conn)", 0, "lib/my/conn.ex", "conn.halt",
                   "Function", 3, "  def halt(conn), do: conn"),
        SL_CALL_AT("use-in-a-nested-module", "lib/my/outer.ex", "outer.in_nested",
                   "assign(conn, 3)", 0, "lib/my/conn.ex", "conn.assign", "Function", 2,
                   "  def assign(conn, key), do: {conn, key}"),
        SL_CALL_AT("import-survives-a-nested-modules-use", "lib/my/outer.ex", "outer.after_nested",
                   "halt(conn)", 1, "lib/my/conn.ex", "conn.halt", "Function", 3,
                   "  def halt(conn), do: conn"),
        SL_CALL_AT("use-holds-to-the-end-of-its-block", "lib/my/scoped.ex", "scoped.in_block",
                   "assign(conn, 1)", 0, "lib/my/conn.ex", "conn.assign", "Function", 2,
                   "  def assign(conn, key), do: {conn, key}"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("except-is-kept", "lib/my/app.ex", "app.no_except", "halt(conn)", 0),
        SL_DENY_AT("what-a-nested-module-adds-ends-with-it", "lib/my/outer.ex",
                   "outer.no_macro_after_nested", "plug(name)", 0),
        SL_DENY_AT("only-is-kept", "lib/my/app.ex", "app.no_only", "not_listed(name)", 0),
        SL_DENY_AT("function-put-here-by-use", "lib/my/app.ex", "app.no_injected",
                   "call(conn, opts)", 0),
        SL_DENY_AT("no-use-no-import", "lib/my/plain.ex", "plain.no_use", "assign(conn, 1)", 0),
        SL_DENY_AT("quote-that-is-not-returned", "lib/my/uses_discards.ex",
                   "uses_discards.no_discarded", "assign(conn, 1)", 0),
        SL_DENY_AT("use-ended-with-its-block", "lib/my/scoped.ex", "scoped.in_block",
                   "assign(conn, 2)", 0),
    };
    ASSERT(sl_check("elixir-use", ".ex", "defmodule Filler do\n  def f, do: 1\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Erlang: fun references ────────────────────────────────────────── */

static const char ERL2_OTHER[] =
    "-module(other).\n"
    "-export([pub/1, imp/1]).\n"
    "pub(X) -> X.\n"
    "imp(X) -> X.\n"
    "hid(X) -> X.\n";
static const char ERL2_REFS[] =
    "-module(refs).\n"
    "-export([local_ref/0, remote_ref/0, imported_ref/0, no_private/0, no_arity/0, no_variable/1]).\n"
    "-import(other, [imp/1]).\n"
    "helper(X) -> X.\n"
    "local_ref() -> fun helper/1.\n"
    "remote_ref() -> fun other:pub/1.\n"
    "imported_ref() -> fun imp/1.\n"
    "no_private() -> fun other:hid/1.\n"
    "no_arity() -> fun helper/2.\n"
    "no_variable(M) -> fun M:pub/1.\n";

TEST(resolver_source_erlang_fun_references) {
    static const RQ_Source sources[] = {{.path = "src/other.erl", .text = ERL2_OTHER},
                                        {.path = "src/refs.erl", .text = ERL2_REFS}};
    static const RQ_Expected expected[] = {
        SL_REF_AT("local-fun-reference", "src/refs.erl", "refs.local_ref", "fun helper/1", 0, "src/refs.erl", "refs.helper", "Function", 4,
                   "helper(X) -> X."),
        SL_REF_AT("remote-fun-reference", "src/refs.erl", "refs.remote_ref", "fun other:pub/1", 0, "src/other.erl", "other.pub", "Function", 3,
                   "pub(X) -> X."),
        SL_REF_AT("imported-fun-reference", "src/refs.erl", "refs.imported_ref", "fun imp/1", 0, "src/other.erl", "other.imp", "Function", 4,
                   "imp(X) -> X."),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_REF_AT("fun-of-a-function-not-exported", "src/refs.erl", "refs.no_private", "fun other:hid/1", 0),
        SL_NO_REF_AT("fun-of-an-absent-arity", "src/refs.erl", "refs.no_arity", "fun helper/2", 0),
        SL_NO_REF_AT("fun-through-a-variable-module", "src/refs.erl", "refs.no_variable", "fun M:pub/1", 0),
    };
    ASSERT(sl_check("erlang-references", ".erl", "-module(filler).\nf() -> 1.\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Perl: the invocant read directly, re-exports and code references ─ */

static const char PL2_BASEUTIL[] =
    "package My::BaseUtil;\n"
    "use Exporter 'import';\n"
    "our @EXPORT_OK = qw(patch);\n"
    "sub patch { return 1; }\n"
    "1;\n";
static const char PL2_UTIL[] =
    "package My::Util;\n"
    "use Exporter 'import';\n"
    "use My::BaseUtil qw(patch);\n"
    "our @EXPORT_OK = qw(patch other);\n"
    "sub other { return 2; }\n"
    "1;\n";
static const char PL2_ROOT[] =
    "package My::Root;\n"
    "sub root_m { return 3; }\n"
    "1;\n";
static const char PL2_THING[] =
    "package My::Thing;\n"
    "use parent -norequire, 'My::Root';\n"
    "use My::Util qw(patch);\n"
    "\n"
    "sub own { return 4; }\n"
    "sub _select { $_[1] ? $_[0]->own : $_[0] }\n"
    "sub via_shift { shift->own }\n"
    "sub via_shift_paren { shift()->own }\n"
    "sub via_arg { $_[0]->own }\n"
    "sub via_inherited { shift->root_m(@_) }\n"
    "sub after_shift { my $x = shift; $_[0]->own }\n"
    "sub second_shift { my $x = shift; shift->own }\n"
    "sub anon_has_its_own_args { my $cb = sub { shift->own }; $cb }\n"
    "sub calls_select { _select(1, 2) }\n"
    "sub calls_reexport { patch() }\n"
    "sub takes_ref { my $r = \\&own; $r }\n"
    "sub takes_qualified_ref { keep(\\&My::Root::root_m) }\n"
    "sub no_absent_ref { my $r = \\&absent; $r }\n"
    "sub also { return 5; }\n"
    "sub picks_one { my $f = shift; keep($f ? \\&own : \\&also) }\n"
    "sub falls_back { my $f = shift; my $r = $f || \\&also; $r }\n"
    "sub low_or { my $f = shift; my $r = ($f or \\&also); $r }\n"
    "sub in_parens { my $f = shift; my $r = $f ? (\\&own) : 0; $r }\n"
    "sub table_entry { my %t = (a => \\&own); $t{a} }\n"
    "1;\n";

static const char PL3_THING[] =
    "package My::Over;\n"
    "use overload bool => sub {1}, '\"\"' => sub { shift->to_text }, fallback => 1;\n"
    "use other_pragma '+' => sub { shift->to_text };\n"
    "\n"
    "sub to_text { return 'x'; }\n"
    "sub list_shift { my ($self, $name) = (shift, shift); $self->to_text; }\n"
    "sub list_shift_second { my ($first, $other) = (shift, shift); $other->to_text; }\n"
    "sub list_not_shift { my ($self, $name) = (1, shift); $self->to_text; }\n"
    "sub list_rebound { my ($self, $name) = (shift, shift); $self = 1; $self->to_text; }\n"
    "1;\n";
static const char PL3_MAIN[] = "use overload '+' => sub { shift->to_text };\n"
                               "sub to_text { return 'main'; }\n";

TEST(resolver_source_perl_list_shift_and_overload) {
    static const RQ_Source sources[] = {
        {.path = "lib/My/Over.pm", .text = PL3_THING},
        {.path = "script/main.pl", .text = PL3_MAIN},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("operator-sub-of-use-overload-gets-the-object", "lib/My/Over.pm", "My.Over",
                   "shift->to_text", 0, "lib/My/Over.pm", "Over.to_text", "Function", 5,
                   "sub to_text { return 'x'; }"),
        SL_CALL_AT("list-of-shifts-takes-the-first-argument", "lib/My/Over.pm", "Over.list_shift",
                   "$self->to_text", 0, "lib/My/Over.pm", "Over.to_text", "Function", 5,
                   "sub to_text { return 'x'; }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("another-pragma-promises-nothing", "lib/My/Over.pm", "My.Over",
                   "shift->to_text", 1),
        SL_DENY_AT("second-of-the-list-is-no-invocant", "lib/My/Over.pm", "Over.list_shift_second",
                   "$other->to_text", 0),
        SL_DENY_AT("list-not-led-by-shift", "lib/My/Over.pm", "Over.list_not_shift",
                   "$self->to_text", 1),
        SL_DENY_AT("rebound-after-the-list", "lib/My/Over.pm", "Over.list_rebound",
                   "$self->to_text", 2),
        SL_DENY_AT("overload-in-main-has-no-class", "script/main.pl", "script.main", "shift->to_text",
                   0),
    };
    ASSERT(sl_check("perl-list-shift", ".pl", "sub filler { return 1; }\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

TEST(resolver_source_perl_invocant_reexports_and_code_references) {
    static const RQ_Source sources[] = {
        {.path = "lib/My/BaseUtil.pm", .text = PL2_BASEUTIL},
        {.path = "lib/My/Util.pm", .text = PL2_UTIL},
        {.path = "lib/My/Root.pm", .text = PL2_ROOT},
        {.path = "lib/My/Thing.pm", .text = PL2_THING},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("shift-is-the-invocant", "lib/My/Thing.pm", "Thing.via_shift", "shift->own", 0, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                   "sub own { return 4; }"),
        SL_CALL_AT("shift-with-parens-is-the-invocant", "lib/My/Thing.pm", "Thing.via_shift_paren", "shift()->own", 0, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                   "sub own { return 4; }"),
        SL_CALL_AT("first-argument-is-the-invocant", "lib/My/Thing.pm", "Thing.via_arg", "$_[0]->own", 1, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                   "sub own { return 4; }"),
        SL_CALL_AT("shift-walks-the-parents", "lib/My/Thing.pm", "Thing.via_inherited", "shift->root_m(@_)", 0, "lib/My/Root.pm", "Root.root_m", "Function", 2,
                   "sub root_m { return 3; }"),
        SL_CALL_AT("import-of-a-reexported-name", "lib/My/Thing.pm", "Thing.calls_reexport", "patch()", 0, "lib/My/BaseUtil.pm", "BaseUtil.patch", "Function", 4,
                   "sub patch { return 1; }"),
        SL_REF_AT("code-reference-to-a-sub-of-the-package", "lib/My/Thing.pm", "Thing.takes_ref", "\\&own", 0, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                   "sub own { return 4; }"),
        SL_REF_AT("code-reference-with-its-package", "lib/My/Thing.pm", "Thing.takes_qualified_ref", "\\&My::Root::root_m", 0, "lib/My/Root.pm", "Root.root_m", "Function", 2,
                   "sub root_m { return 3; }"),
        SL_USE_AT("conditional-branch-mentions-its-sub", "lib/My/Thing.pm", "Thing.picks_one", "\\&own", 1, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                  "sub own { return 4; }"),
        SL_USE_AT("other-conditional-branch-mentions-its-sub", "lib/My/Thing.pm", "Thing.picks_one", "\\&also", 0, "lib/My/Thing.pm", "Thing.also", "Function", 19,
                  "sub also { return 5; }"),
        SL_USE_AT("logical-operand-mentions-its-sub", "lib/My/Thing.pm", "Thing.falls_back", "\\&also", 1, "lib/My/Thing.pm", "Thing.also", "Function", 19,
                  "sub also { return 5; }"),
        SL_USE_AT("low-precedence-operand-mentions-its-sub", "lib/My/Thing.pm", "Thing.low_or", "\\&also", 2, "lib/My/Thing.pm", "Thing.also", "Function", 19,
                  "sub also { return 5; }"),
        SL_USE_AT("parentheses-change-nothing", "lib/My/Thing.pm", "Thing.in_parens", "\\&own", 2, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                  "sub own { return 4; }"),
        SL_REF_AT("table-entry-names-one-sub", "lib/My/Thing.pm", "Thing.table_entry", "\\&own", 3, "lib/My/Thing.pm", "Thing.own", "Function", 5,
                  "sub own { return 4; }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("plain-function-takes-whatever-it-is-given", "lib/My/Thing.pm", "Thing._select", "$_[0]->own", 0),
        SL_DENY_AT("first-argument-after-a-shift-is-the-second", "lib/My/Thing.pm", "Thing.after_shift", "$_[0]->own", 2),
        SL_DENY_AT("second-shift-is-the-second-argument", "lib/My/Thing.pm", "Thing.second_shift", "shift->own", 1),
        SL_DENY_AT("anonymous-sub-has-its-own-arguments", "lib/My/Thing.pm", "Thing.anon_has_its_own_args", "shift->own", 2),
        SL_NO_REF_AT("code-reference-to-nothing", "lib/My/Thing.pm", "Thing.no_absent_ref", "\\&absent", 0),
        SL_NO_REF_AT("conditional-branch-is-no-reference", "lib/My/Thing.pm", "Thing.picks_one", "\\&own", 1),
        SL_NO_REF_AT("other-conditional-branch-is-no-reference", "lib/My/Thing.pm", "Thing.picks_one", "\\&also", 0),
        SL_NO_REF_AT("logical-operand-is-no-reference", "lib/My/Thing.pm", "Thing.falls_back", "\\&also", 1),
        SL_NO_REF_AT("low-precedence-operand-is-no-reference", "lib/My/Thing.pm", "Thing.low_or", "\\&also", 2),
        SL_NO_REF_AT("parentheses-are-no-reference", "lib/My/Thing.pm", "Thing.in_parens", "\\&own", 2),
    };
    ASSERT(sl_check("perl-invocant", ".pm", "package Filler;\nsub f { return 1; }\n1;\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Ruby: inheritance through the header's constant ───────────────── */

static const char RB6_BASE[] = "module App\n"
                               "  class Base\n"
                               "    def run\n"
                               "      1\n"
                               "    end\n"
                               "  end\n"
                               "end\n";
static const char RB6_CHILD[] = "module App\n"
                                "  module Style\n"
                                "    class Child < Base\n"
                                "      def run\n"
                                "        2\n"
                                "      end\n"
                                "    end\n"
                                "    class Outside < Gem::Base\n"
                                "    end\n"
                                "  end\n"
                                "end\n";

TEST(resolver_source_ruby_bare_new_and_index_operators) {
    static const RQ_Source sources[] = {{.path = RB6_FILE, .text = RB6_INDEX},
                                        {.path = "lib/app/corrector.rb", .text = RB7_CORRECTOR}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("include-in-a-singleton-class-is-extend", "lib/app/corrector.rb",
                   "Corrector.correct", "from_helper()", 0, "lib/app/corrector.rb",
                   "Helper.from_helper", "Method", 2, "  def from_helper"),
        RB6_CALL("bare-new-in-a-class-method", "build", "new(a)", 0, "initialize", 2,
                 "  def initialize(a = 1)"),
        RB6_CALL("bare-new-under-operator-assignment", "memo", "new(a)", 1, "initialize", 2,
                 "  def initialize(a = 1)"),
        RB6_CALL("index-reads-self", "reads_self", "self[key]", 0, "[]", 6, "  def [](key)"),
        RB6_CALL("index-writes-self", "writes_self", "self[key]", 1, "[]=", 10,
                 "  def []=(key, value)"),
        RB6_CALL("index-or-assign-reads", "both_self", "self[key]", 2, "[]", 6, "  def [](key)"),
        RB6_CALL("index-or-assign-writes", "both_self", "[", 4, "[]=", 10,
                 "  def []=(key, value)"),
        RB6_CALL("new-of-a-constant", "reads_typed", "Store.new", 0, "initialize", 2,
                 "  def initialize(a = 1)"),
        RB6_CALL("index-of-a-local-from-new", "reads_typed", "other[1]", 0, "[]", 6,
                 "  def [](key)"),
        SL_USE_AT("constant-in-new", RB6_FILE, "Store.reads_typed", "Store", 1, RB6_FILE,
                  "app.store.Store", "Class", 1, "class Store"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("an-instance-does-not-have-it", "lib/app/corrector.rb",
                   "Corrector.no_instance_reach", "from_helper()", 1),
        SL_DENY_AT("new-is-no-instance-method", RB6_FILE, "Store.no_instance_new", "new(a)", 3),
        SL_DENY_AT("index-of-an-unknown-value", RB6_FILE, "Store.no_unknown_receiver", "thing[1]",
                   0),
        SL_DENY_AT("index-of-a-literal", RB6_FILE, "Store.no_array_literal", "[1][0]", 0),
        SL_DENY_AT("index-absent-from-the-chain", RB6_FILE, "Plain.no_index_on_a_class_without_it",
                   "self[1]", 0),
    };
    ASSERT(sl_check("ruby-index", ".rb", "class Filler\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

TEST(resolver_source_ruby_super_selects_what_follows_the_own_step) {
    static const RQ_Source sources[] = {{.path = "lib/app/kin.rb", .text = RB8_SUPER}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("super-reaches-the-superclass", "lib/app/kin.rb", "Child.go", "super", 0,
                   "lib/app/kin.rb", "Parent.go", "Method", 8, "  def go(a)"),
        SL_CALL_AT("super-with-parens-reaches-an-included-module-first", "lib/app/kin.rb",
                   "Child.through_mixin", "super", 1, "lib/app/kin.rb", "Mixed.through_mixin",
                   "Method", 2, "  def through_mixin"),
        SL_CALL_AT("super-of-a-class-method-reaches-the-superclasss", "lib/app/kin.rb", "Child.make",
                   "super", 2, "lib/app/kin.rb", "Parent.make", "Method", 12, "  def self.make"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("nothing-on-the-chain", "lib/app/kin.rb", "Child.no_parent_method", "super", 3),
        SL_DENY_AT("chain-leaves-the-project", "lib/app/kin.rb", "Outsider.no_chain_out", "super", 4),
        SL_DENY_AT("a-module-method-goes-where-its-includer-says", "lib/app/kin.rb",
                   "Mod.no_module_knows", "super", 5),
    };
    ASSERT(sl_check("ruby-super", ".rb", "class Filler\nend\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

TEST(resolver_source_ruby_superclass_reaches_the_graph_on_both_routes) {
    for (int driver = 0; driver < 2; driver++) {
        const char *root = th_mktempdir("cbm_source_langs");
        ASSERT_NOT_NULL(root);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", root);
        snprintf(database, sizeof(database), "%s/index.db", root);
        ASSERT(cbm_mkdir_p(repo, 0755));
        RQ_Source base = {.path = "lib/app/base.rb", .text = RB6_BASE};
        RQ_Source child = {.path = "lib/app/style/child.rb", .text = RB6_CHILD};
        ASSERT(sl_write(repo, &base) && sl_write(repo, &child));
        for (int i = 0; i < 55; i++) {
            char path[64];
            snprintf(path, sizeof(path), "filler/f%02d.rb", i);
            RQ_Source filler = {.path = path, .text = "def filler\n  1\nend\n"};
            ASSERT(sl_write(repo, &filler));
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
        ASSERT(pipeline && cbm_pipeline_set_project_name(pipeline, "ruby-inherit") &&
               cbm_pipeline_run(pipeline) == 0);
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else {
            cbm_unsetenv("CBM_WORKERS");
        }
        cbm_store_t *store = cbm_store_open_path_existing(database);
        ASSERT_NOT_NULL(store);
        /* Child inherits Base through the enclosing module; Outside's parent is a gem's. */
        ASSERT_EQ(cbm_store_count_edges_by_type(store, "ruby-inherit", "INHERITS"), 1);
        ASSERT_EQ(cbm_store_count_edges_by_type(store, "ruby-inherit", "OVERRIDE"), 1);
        cbm_store_close(store);
        th_cleanup(root);
    }
    PASS();
}

/* ── OCaml: dune libraries, includes and scoped opens ──────────────── */

static const char ML_LIB_DUNE[] =
    "(include_subdirs qualified)\n"
    "\n"
    "(library\n"
    " (name corelib)\n"
    " (public_name corelib)\n"
    " (synopsis \"A library.\\nWith a string (holding parentheses.\")\n"
    " (libraries unix))\n";
static const char ML_CORELIB[] =
    "let printfn x = x\n"
    "module Path = Path\n"
    "module Local = struct\n"
    "  let inner x = x\n"
    "end\n";
static const char ML_PATH[] =
    "let to_string x = x\n";
static const char ML_NOMAIN_DUNE[] =
    "(library\n"
    " (name nomain))\n";
static const char ML_NOMAIN_UNIT[] =
    "let member x = x\n";
static const char ML_FUNCTORED_DUNE[] =
    "(library\n"
    " (name functored))\n";
static const char ML_FUNCTORED[] =
    "include Make (Base)\n"
    "\n"
    "let known x = x\n";
static const char ML_RULES_DUNE[] =
    "(include_subdirs unqualified)\n"
    "\n"
    "(library\n"
    " (name rules)\n"
    " (libraries corelib nomain functored))\n";
static const char ML_IMPORT[] =
    "include Corelib\n"
    "\n"
    "include struct\n"
    "  open Nomain\n"
    "  module Unit_one = Unit_one\n"
    "end\n"
    "\n"
    "let helper x = x\n";
static const char ML_DEEP[] =
    "let deep x = x\n";
static const char ML_DUNE_MAIN[] =
    "open Import\n"
    "\n"
    "let bare_from_open x = helper x\n"
    "let bare_through_include x = printfn x\n"
    "let module_through_include x = Path.to_string x\n"
    "let module_from_subtree x = Deep.deep x\n"
    "let library_path x = Corelib.Local.inner x\n"
    "let wrapped_library x = Nomain.Unit_one.member x\n"
    "let alias_under_struct_open x = Unit_one.member x\n"
    "let no_module_outside x = Outside.thing x\n"
    "let no_local_record_field { helper; _ } = helper 1\n"
    "let no_local_parameter ~helper = helper 1\n"
    "let no_shadowed_by_local_open x = Corelib.(printfn x)\n"
    "let local_open_is_scoped x = helper x\n";
static const char ML_OTHER[] =
    "open Functored\n"
    "\n"
    "let no_after_open_ended x = known x\n";
static const char ML_SHADOW[] =
    "let helper x = x\n"
    "let call_before x = helper x\n"
    "\n"
    "open Corelib\n"
    "\n"
    "let printfn x = x\n"
    "let own_after_open x = printfn x\n";

TEST(resolver_source_ocaml_dune_libraries_includes_and_scoped_opens) {
    static const RQ_Source sources[] = {
        {.path = "libs/corelib/dune", .text = ML_LIB_DUNE},
        {.path = "libs/corelib/corelib.ml", .text = ML_CORELIB},
        {.path = "libs/corelib/path.ml", .text = ML_PATH},
        {.path = "libs/nomain/dune", .text = ML_NOMAIN_DUNE},
        {.path = "libs/nomain/unit_one.ml", .text = ML_NOMAIN_UNIT},
        {.path = "libs/functored/dune", .text = ML_FUNCTORED_DUNE},
        {.path = "libs/functored/functored.ml", .text = ML_FUNCTORED},
        {.path = "src/rules/dune", .text = ML_RULES_DUNE},
        {.path = "src/rules/import.ml", .text = ML_IMPORT},
        {.path = "src/rules/sub/deep.ml", .text = ML_DEEP},
        {.path = "src/rules/main.ml", .text = ML_DUNE_MAIN},
        {.path = "src/other/other.ml", .text = ML_OTHER},
        {.path = "src/rules/shadow.ml", .text = ML_SHADOW},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("name-from-an-open-sibling", "src/rules/main.ml", "main.bare_from_open", "helper x", 0, "src/rules/import.ml", "import.helper", "Function", 8,
                   "let helper x = x"),
        SL_CALL_AT("name-through-an-include-of-a-library", "src/rules/main.ml", "main.bare_through_include", "printfn x", 0, "libs/corelib/corelib.ml", "corelib.printfn", "Function", 1,
                   "let printfn x = x"),
        SL_CALL_AT("module-through-an-include-of-a-library", "src/rules/main.ml", "main.module_through_include", "Path.to_string x", 0, "libs/corelib/path.ml", "path.to_string", "Function", 1,
                   "let to_string x = x"),
        SL_CALL_AT("unit-of-an-unqualified-subtree", "src/rules/main.ml", "main.module_from_subtree", "Deep.deep x", 0, "src/rules/sub/deep.ml", "deep.deep", "Function", 1,
                   "let deep x = x"),
        SL_CALL_AT("nested-module-of-a-library", "src/rules/main.ml", "main.library_path", "Corelib.Local.inner x", 0, "libs/corelib/corelib.ml", "corelib.Local.inner", "Function", 4,
                   "  let inner x = x"),
        SL_CALL_AT("unit-of-a-library-with-no-main-module", "src/rules/main.ml", "main.wrapped_library", "Nomain.Unit_one.member x", 0, "libs/nomain/unit_one.ml", "unit_one.member", "Function", 1,
                   "let member x = x"),
        SL_CALL_AT("alias-read-under-its-structs-open", "src/rules/main.ml", "main.alias_under_struct_open", "Unit_one.member x", 1, "libs/nomain/unit_one.ml", "unit_one.member", "Function", 1,
                   "let member x = x"),
        SL_CALL_AT("local-open-selects-from-its-module", "src/rules/main.ml", "main.no_shadowed_by_local_open", "printfn x", 1, "libs/corelib/corelib.ml", "corelib.printfn", "Function", 1,
                   "let printfn x = x"),
        SL_CALL_AT("local-open-ends-with-its-expression", "src/rules/main.ml", "main.local_open_is_scoped", "helper x", 1, "src/rules/import.ml", "import.helper", "Function", 8,
                   "let helper x = x"),
        SL_CALL_AT("let-before-an-open-it-is-not-in", "src/rules/shadow.ml", "shadow.call_before", "helper x", 1, "src/rules/shadow.ml", "shadow.helper", "Function", 1,
                   "let helper x = x"),
        SL_CALL_AT("let-after-an-open-replaces-its-name", "src/rules/shadow.ml", "shadow.own_after_open", "printfn x", 1, "src/rules/shadow.ml", "shadow.printfn", "Function", 6,
                   "let printfn x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("module-outside-the-project", "src/rules/main.ml", "main.no_module_outside", "Outside.thing x", 0),
        SL_DENY_AT("record-pun-binds-the-field", "src/rules/main.ml", "main.no_local_record_field", "helper 1", 0),
        SL_DENY_AT("labelled-parameter-binds", "src/rules/main.ml", "main.no_local_parameter", "helper 1", 1),
        SL_DENY_AT("open-of-a-functor-application-is-unknown", "src/other/other.ml", "other.no_after_open_ended", "known x", 0),
    };
    ASSERT(sl_check("ocaml-dune", ".ml", "let filler x = x\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

static const char ML2_BELOW[] = "open Rootimport\n"
                                "let up_from_a_subdirectory x = at_the_root x\n"
                                "let public_name_is_the_library x = Publiclib.from_public x\n"
                                "let no_dotted_public_name x = Dotted.from_dotted x\n"
                                "let no_public_beside_a_name x = Other_name.from_real x\n"
                                "let name_beside_a_public_name x = Real_name.from_real x\n";

TEST(resolver_source_ocaml_subdirectories_and_public_names) {
    static const RQ_Source sources[] = {
        {.path = "libs/publiclib/dune", .text = "(library\n (public_name publiclib))\n"},
        {.path = "libs/publiclib/publiclib.ml", .text = "let from_public x = x\n"},
        {.path = "libs/dotted/dune", .text = "(library\n (public_name some.dotted))\n"},
        {.path = "libs/dotted/dotted.ml", .text = "let from_dotted x = x\n"},
        {.path = "libs/named/dune",
         .text = "(library\n (name real_name)\n (public_name other_name))\n"},
        {.path = "libs/named/real_name.ml", .text = "let from_real x = x\n"},
        {.path = "src/rules/dune", .text = "(include_subdirs unqualified)\n\n(library\n (name rules))\n"},
        {.path = "src/rules/rootimport.ml", .text = "let at_the_root x = x\n"},
        {.path = "src/rules/sub/below.ml", .text = ML2_BELOW},
        {.path = "src/rules/sub/paired.ml", .text = "let in_a_subdirectory x = x\n"},
        {.path = "src/rules/sub/paired.mli", .text = "val in_a_subdirectory : int -> int\n"},
        {.path = "src/rules/top.ml",
         .text = "let down_to_a_unit_with_an_interface x = Paired.in_a_subdirectory x\n"},
        {.path = "src/plain/dune", .text = "(library\n (name plain))\n"},
        {.path = "src/plain/top.ml", .text = "let at_top x = x\n"},
        {.path = "src/plain/sub/inner.ml", .text = "let no_climb_without_subdirs x = Top.at_top x\n"},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("subdirectory-reads-the-librarys-root", "src/rules/sub/below.ml",
                   "below.up_from_a_subdirectory", "at_the_root x", 0, "src/rules/rootimport.ml",
                   "rootimport.at_the_root", "Function", 1, "let at_the_root x = x"),
        SL_CALL_AT("unit-with-an-interface-under-unqualified", "src/rules/top.ml",
                   "top.down_to_a_unit_with_an_interface", "Paired.in_a_subdirectory x", 0,
                   "src/rules/sub/paired.ml", "paired.in_a_subdirectory", "Function", 1,
                   "let in_a_subdirectory x = x"),
        SL_CALL_AT("public-name-with-no-name", "src/rules/sub/below.ml",
                   "below.public_name_is_the_library", "Publiclib.from_public x", 0,
                   "libs/publiclib/publiclib.ml", "publiclib.from_public", "Function", 1,
                   "let from_public x = x"),
        SL_CALL_AT("name-beside-a-public-name", "src/rules/sub/below.ml",
                   "below.name_beside_a_public_name", "Real_name.from_real x", 0,
                   "libs/named/real_name.ml", "real_name.from_real", "Function", 1,
                   "let from_real x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("dotted-public-name-is-no-module", "src/rules/sub/below.ml",
                   "below.no_dotted_public_name", "Dotted.from_dotted x", 0),
        SL_DENY_AT("public-name-beside-a-name", "src/rules/sub/below.ml",
                   "below.no_public_beside_a_name", "Other_name.from_real x", 0),
        SL_DENY_AT("no-climb-without-include-subdirs", "src/plain/sub/inner.ml",
                   "inner.no_climb_without_subdirs", "Top.at_top x", 0),
    };
    ASSERT(sl_check("ocaml-subdirs", ".ml", "let filler x = x\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* An include is read under the opens in force. One in an interface that cannot be read leaves
 * the implementation to answer. */
static const char ML4_BELOW[] = "open Base_lib\n"
                                "\n"
                                "include Util\n"
                                "\n"
                                "module Util = struct\n"
                                "  include Util\n"
                                "  let own_after x = x\n"
                                "end\n"
                                "\n"
                                "let through_an_opened_include x = from_util x\n"
                                "let through_a_nested_include x = Util.from_util x\n"
                                "let declared_after_the_include x = Util.own_after x\n";
static const char ML4_IFACE_ML[] = "let declared x = x\n";
static const char ML4_IFACE_MLI[] = "include Some_sig with type t := t\n"
                                    "val declared : int -> int\n";
static const char ML4_USER[] = "let through_the_implementation x = Iface.declared x\n"
                               "let no_such_member x = Iface.absent x\n";

TEST(resolver_source_ocaml_includes_under_opens_and_interfaces) {
    static const RQ_Source sources[] = {
        {.path = "libs/base_lib/dune", .text = "(library\n (name base_lib))\n"},
        {.path = "libs/base_lib/base_lib.ml", .text = "module Util = Util\n"},
        {.path = "libs/base_lib/util.ml", .text = "let from_util x = x\n"},
        {.path = "src/below.ml", .text = ML4_BELOW},
        {.path = "src/iface.ml", .text = ML4_IFACE_ML},
        {.path = "src/iface.mli", .text = ML4_IFACE_MLI},
        {.path = "src/user.ml", .text = ML4_USER},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("include-read-under-an-open", "src/below.ml", "below.through_an_opened_include",
                   "from_util x", 0, "libs/base_lib/util.ml", "util.from_util", "Function", 1,
                   "let from_util x = x"),
        SL_CALL_AT("nested-include-of-its-own-name", "src/below.ml",
                   "below.through_a_nested_include", "Util.from_util x", 0, "libs/base_lib/util.ml",
                   "util.from_util", "Function", 1, "let from_util x = x"),
        SL_CALL_AT("declared-after-an-include", "src/below.ml",
                   "below.declared_after_the_include", "Util.own_after x", 0, "src/below.ml",
                   "below.Util.own_after", "Function", 7, "  let own_after x = x"),
        SL_CALL_AT("interface-include-adds-no-doubt", "src/user.ml",
                   "user.through_the_implementation", "Iface.declared x", 0, "src/iface.ml",
                   "iface.declared", "Function", 1, "let declared x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("member-absent-from-the-implementation", "src/user.ml", "user.no_such_member",
                   "Iface.absent x", 0),
    };
    ASSERT(sl_check("ocaml-includes", ".ml", "let filler x = x\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* What a functor's result has, what its parameter has, what an interface shows. */
static const char ML5_MONAD[] = "module type Basic = Monad_intf.Basic\n"
                                "\n"
                                "module Make (M : Basic) = struct\n"
                                "  include M\n"
                                "\n"
                                "  let map t ~f = bind t ~f:(fun x -> return (f x))\n"
                                "\n"
                                "  module O = struct\n"
                                "    let ( let* ) t f = bind t ~f\n"
                                "    let both a b = map a ~f:(fun x -> x, b)\n"
                                "  end\n"
                                "end\n";
static const char ML5_MONAD_INTF[] = "module type Basic = sig\n"
                                     "  type 'a t\n"
                                     "\n"
                                     "  val return : 'a -> 'a t\n"
                                     "  val bind : 'a t -> f:('a -> 'b t) -> 'b t\n"
                                     "end\n";
static const char ML5_RESULT_LIKE[] = "open Core_lib\n"
                                      "\n"
                                      "let return x = Ok x\n"
                                      "let bind t ~f = Result.bind t f\n"
                                      "\n"
                                      "include Monad.Make (struct\n"
                                      "    type nonrec 'a t = ('a, string) result\n"
                                      "\n"
                                      "    let return = return\n"
                                      "    let bind = bind\n"
                                      "  end)\n"
                                      "\n"
                                      "let after_the_application x = Helper.run x\n"
                                      "let what_the_functor_made x = map x ~f:Helper.run\n";
static const char ML5_WRAP[] = "module Make (Helper : sig val run : int -> int end) = struct\n"
                               "  let through_the_parameter x = Helper.run x\n"
                               "end\n";
static const char ML5_COMPARED[] = "let compare a b = a - b\n"
                                   "\n"
                                   "include Core_lib.Comparable.Make (struct\n"
                                   "    type t = int\n"
                                   "\n"
                                   "    let compare a b = a + b\n"
                                   "    let uses_its_own a b = compare a b\n"
                                   "  end)\n";
static const char ML5_APPLIED[] = "module C = Core_lib.Comparable.Make (struct\n"
                                  "    type t = int\n"
                                  "\n"
                                  "    let compare = Stdlib.compare\n"
                                  "    let uses_a_helper x = Core_lib.Helper.run x\n"
                                  "  end)\n"
                                  "\n"
                                  "let through_an_applied_module a b = C.max_of a b\n";
static const char ML5_LATER[] = "open Core_lib\n"
                                "module Helper = struct let run x = x + 1 end\n"
                                "module Alias = Helper\n"
                                "\n"
                                "let through_the_binding_below_the_open x = Alias.run x\n";
static const char ML5_STRUCT[] = "open struct\n"
                                 "  open Outside\n"
                                 "  let private_helper x = x\n"
                                 "end\n"
                                 "\n"
                                 "let after_an_open_struct x = private_helper x\n"
                                 "let nothing_is_left_open x = Core_lib.Helper.run x\n"
                                 "\n"
                                 "include struct\n"
                                 "  open Outside\n"
                                 "  let another x = x\n"
                                 "end\n"
                                 "\n"
                                 "let an_inlined_structs_open_ends x = Core_lib.Helper.run x\n"
                                 "\n"
                                 "include struct\n"
                                 "  include Outside\n"
                                 "end\n"
                                 "\n"
                                 "let what_it_includes_stays x = Core_lib.Helper.run x\n";
#define ML5_HELPER_RUN(id, file, caller, text, at)                                                 \
    SL_CALL_AT(id, file, caller, text, at, "libs/core_lib/helper.ml", "helper.run", "Function", 1, \
               "let run x = x")

TEST(resolver_source_ocaml_functors_module_types_and_interfaces) {
    static const RQ_Source sources[] = {
        {.path = "libs/core_lib/dune", .text = "(library\n (name core_lib))\n"},
        {.path = "libs/core_lib/core_lib.ml",
         .text = "module Comparable = Comparable\n"
                 "module Helper = Helper\n"
                 "module Hidden = Hidden\n"
                 "module List = List\n"
                 "module Listed = Listed\n"
                 "module Loosely = Loosely\n"
                 "module Monad = Monad\n"
                 "module Opaque = Opaque\n"
                 "module Replaced = Replaced\n"
                 "module Holds = Holds\n"
                 "module Pp = Pp\n"
                 "module Sealed = Sealed\n"
                 "module Unsealed = Unsealed\n"
                 "module Sexp0 = Sexp0\n"},
        {.path = "libs/core_lib/helper.ml", .text = "let run x = x\n"},
        {.path = "libs/core_lib/monad.ml", .text = ML5_MONAD},
        {.path = "libs/core_lib/monad_intf.ml", .text = ML5_MONAD_INTF},
        {.path = "libs/core_lib/comparable.ml",
         .text = "module Make (Key : sig type t val compare : t -> t -> int end) = struct\n"
                 "  let max_of a b = if Key.compare a b > 0 then a else b\n"
                 "end\n"},
        {.path = "libs/core_lib/wrap.ml", .text = ML5_WRAP},
        {.path = "libs/core_lib/opaque.ml",
         .text = "module Make (X : Outside.S) = struct\n  include X\nend\n"},
        {.path = "libs/core_lib/holds.ml",
         .text = "module Make (X : sig module Helper : sig val run : int -> int end end) = struct\n"
                 "  include X\n"
                 "end\n"},
        {.path = "libs/core_lib/hidden.ml",
         .text = "module Helper = struct let run x = x + 1 end\n"
                 "let shown x = x\n"
                 "let not_shown x = x\n"},
        {.path = "libs/core_lib/hidden.mli", .text = "val shown : int -> int\n"},
        {.path = "libs/core_lib/sealed.ml", .text = "include Outside\nlet shown x = x\n"},
        {.path = "libs/core_lib/sealed.mli", .text = "val shown : int -> int\n"},
        {.path = "libs/core_lib/unsealed.ml", .text = "include Outside\nlet shown x = x\n"},
        {.path = "libs/core_lib/listed.ml",
         .text = "module Inner = struct let in_a_listed_module x = x end\n"},
        {.path = "libs/core_lib/listed.mli",
         .text = "module Inner : module type of Somewhere_else\n"},
        {.path = "src/listed_user.ml",
         .text = "let a_listed_module_of_no_known_type x = Core_lib.Listed.Inner.in_a_listed_module "
                 "x\n"},
        {.path = "libs/core_lib/loosely.ml", .text = "let shown x = x\n"},
        {.path = "libs/core_lib/loosely.mli",
         .text = "include Some_sig with type t := t\nval shown : int -> int\n"},
        {.path = "src/loosely_user.ml",
         .text = "open Core_lib\n"
                 "open Loosely\n"
                 "\n"
                 "let closed_by_what_it_implements x = Helper.run x\n"},
        {.path = "libs/core_lib/replaced.ml",
         .text = "let early x = x\n"
                 "include String\n"
                 "include Comparable.Make (struct type t = int let compare a b = a - b end)\n"
                 "let late x = x\n"},
        {.path = "src/replaced_user.ml",
         .text = "let the_latest_binding_is_known a b = Core_lib.Replaced.max_of a b\n"
                 "let declared_below_what_is_unknown x = Core_lib.Replaced.late x\n"
                 "let an_unknown_include_may_replace_it x = Core_lib.Replaced.early x\n"},
        {.path = "libs/core_lib/sexp0.ml",
         .text = "module Decoder = struct let decode x = x end\n"},
        {.path = "libs/core_lib/list.ml", .text = "include List\nlet own_list x = x\n"},
        {.path = "libs/core_lib/pp.ml", .text = "include Pp\nlet own_pp x = x\n"},
        {.path = "libs/pp/dune", .text = "(library\n (name pp))\n"},
        {.path = "libs/pp/pp.ml", .text = "let text x = x\n"},
        /* A cram test's directory holds a project of its own, by any name. */
        {.path = "test/cases/one.t/dune", .text = "(library\n (name core_lib))\n"},
        {.path = "test/cases/one.t/core_lib.ml", .text = "let in_a_cram_test x = x\n"},
        {.path = "src/result_like.ml", .text = ML5_RESULT_LIKE},
        {.path = "src/user.ml",
         .text = "open Core_lib\n"
                 "open Result_like.O\n"
                 "\n"
                 "let below_a_functors_module x = Helper.run x\n"
                 "let from_a_functors_module a b = both a b\n"},
        {.path = "src/opaque_user.ml",
         .text = "open Core_lib\n"
                 "include Opaque.Make (struct end)\n"
                 "\n"
                 "let a_parameter_no_one_can_read x = Helper.run x\n"},
        {.path = "src/holds_user.ml",
         .text = "open Core_lib\n"
                 "include Holds.Make (struct module Helper = struct let run x = x end end)\n"
                 "\n"
                 "let a_parameter_that_says_it_has_it x = Helper.run x\n"},
        {.path = "src/compared.ml", .text = ML5_COMPARED},
        {.path = "src/applied.ml", .text = ML5_APPLIED},
        {.path = "src/later.ml", .text = ML5_LATER},
        {.path = "src/opened_struct.ml", .text = ML5_STRUCT},
        {.path = "src/startup.ml",
         .text = "let twice x = Core_lib.Helper.run x\n"
                 "let twice x = Core_lib.Helper.run x\n"},
        {.path = "src/hidden_user.ml",
         .text = "open Core_lib\n"
                 "let not_shown x = x\n"
                 "open Hidden\n"
                 "\n"
                 "let past_what_an_interface_keeps x = Helper.run x\n"
                 "let earlier_name_is_not_replaced x = not_shown x\n"
                 "let what_an_interface_shows x = shown x\n"},
        {.path = "src/sealed_user.ml",
         .text = "open Core_lib\n"
                 "open Sealed\n"
                 "\n"
                 "let through_a_sealed_open x = Helper.run x\n"},
        {.path = "src/unsealed_user.ml",
         .text = "open Core_lib\n"
                 "open Unsealed\n"
                 "\n"
                 "let an_include_with_no_interface x = Helper.run x\n"},
        {.path = "src/import.ml",
         .text = "include Core_lib\n"
                 "\n"
                 "include struct\n"
                 "  open Sexp0\n"
                 "  module Decoder = Decoder\n"
                 "end\n"},
        {.path = "src/decoding.ml",
         .text = "open Import\n"
                 "open Decoder\n"
                 "\n"
                 "let through_an_include_above x = decode x\n"
                 "let past_it x = Helper.run x\n"},
        {.path = "src/pp_user.ml", .text = "let not_the_unit_itself x = Core_lib.Pp.text x\n"},
        {.path = "src/memprof.ml", .text = "let start x = x\n"},
        {.path = "src/printing.ml",
         .text = "open Core_lib\n"
                 "open Helper\n"
                 "open Printf\n"
                 "\n"
                 "let a_standard_module_holds_no_module x = Helper.run x\n"
                 "let its_values_are_not_listed x = run x\n"},
        {.path = "src/collecting.ml",
         .text = "open Gc\n"
                 "\n"
                 "let a_standard_module_holds_this_one x = Memprof.start x\n"},
        {.path = "src/list_user.ml",
         .text = "open Core_lib\n"
                 "open List\n"
                 "\n"
                 "let through_a_list_of_its_own x = Helper.run x\n"
                 "let its_own_value x = own_list x\n"},
    };
    static const RQ_Expected expected[] = {
        ML5_HELPER_RUN("functor-result-holds-no-such-module", "src/result_like.ml",
                       "result_like.after_the_application", "Helper.run x", 0),
        SL_CALL_AT("functor-result-value", "src/result_like.ml",
                   "result_like.what_the_functor_made", "map x ~f:Helper.run", 0,
                   "libs/core_lib/monad.ml", "monad.Make.map", "Function", 6,
                   "  let map t ~f = bind t ~f:(fun x -> return (f x))"),
        ML5_HELPER_RUN("open-of-a-module-a-functor-makes", "src/user.ml",
                       "user.below_a_functors_module", "Helper.run x", 0),
        SL_CALL_AT("value-of-a-module-a-functor-makes", "src/user.ml",
                   "user.from_a_functors_module", "both a b", 0, "libs/core_lib/monad.ml",
                   "monad.Make.O.both", "Function", 10,
                   "    let both a b = map a ~f:(fun x -> x, b)"),
        SL_CALL_AT("applied-module", "src/applied.ml", "applied.through_an_applied_module",
                   "C.max_of a b", 0, "libs/core_lib/comparable.ml", "comparable.Make.max_of",
                   "Function", 2, "  let max_of a b = if Key.compare a b > 0 then a else b"),
        ML5_HELPER_RUN("inside-a-functors-argument", "src/applied.ml", "applied.C.uses_a_helper",
                       "Core_lib.Helper.run x", 0),
        SL_CALL_AT("binding-below-an-open-comes-first", "src/later.ml",
                   "later.through_the_binding_below_the_open", "Alias.run x", 0, "src/later.ml",
                   "later.Helper.run", "Function", 2,
                   "module Helper = struct let run x = x + 1 end"),
        SL_CALL_AT("open-struct-names", "src/opened_struct.ml",
                   "opened_struct.after_an_open_struct", "private_helper x", 1,
                   "src/opened_struct.ml", "opened_struct.private_helper", "Function", 3,
                   "  let private_helper x = x"),
        ML5_HELPER_RUN("an-inlined-structs-open-ends", "src/opened_struct.ml",
                       "opened_struct.an_inlined_structs_open_ends", "Core_lib.Helper.run x", 1),
        ML5_HELPER_RUN("open-struct-leaves-nothing-open", "src/opened_struct.ml",
                       "opened_struct.nothing_is_left_open", "Core_lib.Helper.run x", 0),
        ML5_HELPER_RUN("past-what-an-interface-keeps", "src/hidden_user.ml",
                       "hidden_user.past_what_an_interface_keeps", "Helper.run x", 0),
        SL_CALL_AT("earlier-name-not-replaced-by-a-hidden-one", "src/hidden_user.ml",
                   "hidden_user.earlier_name_is_not_replaced", "not_shown x", 1,
                   "src/hidden_user.ml", "hidden_user.not_shown", "Function", 2,
                   "let not_shown x = x"),
        SL_CALL_AT("what-an-interface-shows", "src/hidden_user.ml",
                   "hidden_user.what_an_interface_shows", "shown x", 2, "libs/core_lib/hidden.ml",
                   "hidden.shown", "Function", 2, "let shown x = x"),
        SL_CALL_AT("a-listed-module-of-no-known-type", "src/listed_user.ml",
                   "listed_user.a_listed_module_of_no_known_type",
                   "Core_lib.Listed.Inner.in_a_listed_module x", 0, "libs/core_lib/listed.ml",
                   "listed.Inner.in_a_listed_module", "Function", 1,
                   "module Inner = struct let in_a_listed_module x = x end"),
        ML5_HELPER_RUN("a-closed-implementation-closes-the-unit", "src/loosely_user.ml",
                       "loosely_user.closed_by_what_it_implements", "Helper.run x", 0),
        ML5_HELPER_RUN("a-closed-interface-closes-the-unit", "src/sealed_user.ml",
                       "sealed_user.through_a_sealed_open", "Helper.run x", 0),
        SL_CALL_AT("an-include-above-reaches-an-alias", "src/decoding.ml",
                   "decoding.through_an_include_above", "decode x", 0, "libs/core_lib/sexp0.ml",
                   "sexp0.Decoder.decode", "Function", 1,
                   "module Decoder = struct let decode x = x end"),
        ML5_HELPER_RUN("past-the-opens-of-an-include", "src/decoding.ml", "decoding.past_it",
                       "Helper.run x", 0),
        SL_CALL_AT("a-unit-is-not-in-scope-in-itself", "src/pp_user.ml",
                   "pp_user.not_the_unit_itself", "Core_lib.Pp.text x", 0, "libs/pp/pp.ml",
                   "pp.text", "Function", 1, "let text x = x"),
        SL_CALL_AT("a-functors-binding-below-an-unknown-include", "src/replaced_user.ml",
                   "replaced_user.the_latest_binding_is_known", "Core_lib.Replaced.max_of a b", 0,
                   "libs/core_lib/comparable.ml", "comparable.Make.max_of", "Function", 2,
                   "  let max_of a b = if Key.compare a b > 0 then a else b"),
        SL_CALL_AT("own-binding-below-an-unknown-include", "src/replaced_user.ml",
                   "replaced_user.declared_below_what_is_unknown", "Core_lib.Replaced.late x", 0,
                   "libs/core_lib/replaced.ml", "replaced.late", "Function", 4, "let late x = x"),
        ML5_HELPER_RUN("standard-module-holds-no-module", "src/printing.ml",
                       "printing.a_standard_module_holds_no_module", "Helper.run x", 0),
        ML5_HELPER_RUN("include-of-the-standard-one", "src/list_user.ml",
                       "list_user.through_a_list_of_its_own", "Helper.run x", 0),
        SL_CALL_AT("own-value-beside-it", "src/list_user.ml", "list_user.its_own_value",
                   "own_list x", 0, "libs/core_lib/list.ml", "list.own_list", "Function", 2,
                   "let own_list x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("a-parameter-is-not-the-unit-of-its-name", "libs/core_lib/wrap.ml",
                   "wrap.Make.through_the_parameter", "Helper.run x", 0),
        SL_DENY_AT("a-parameter-no-one-can-read", "src/opaque_user.ml",
                   "opaque_user.a_parameter_no_one_can_read", "Helper.run x", 0),
        SL_DENY_AT("a-parameter-that-says-it-has-it", "src/holds_user.ml",
                   "holds_user.a_parameter_that_says_it_has_it", "Helper.run x", 0),
        SL_DENY_AT("a-functor-arguments-names-are-its-own", "src/compared.ml",
                   "compared.uses_its_own", "compare a b", 2),
        SL_DENY_AT("two-lets-of-one-name-are-still-two", "src/startup.ml", "startup.twice",
                   "Core_lib.Helper.run x", 0),
        SL_DENY_AT("the-second-of-two-lets-of-one-name", "src/startup.ml", "startup.twice",
                   "Core_lib.Helper.run x", 1),
        SL_DENY_AT("what-an-inlined-struct-includes-stays", "src/opened_struct.ml",
                   "opened_struct.what_it_includes_stays", "Core_lib.Helper.run x", 2),
        SL_DENY_AT("an-unknown-include-below-a-binding", "src/replaced_user.ml",
                   "replaced_user.an_unknown_include_may_replace_it", "Core_lib.Replaced.early x",
                   0),
        SL_DENY_AT("an-include-with-no-interface", "src/unsealed_user.ml",
                   "unsealed_user.an_include_with_no_interface", "Helper.run x", 0),
        SL_DENY_AT("standard-values-are-not-listed", "src/printing.ml",
                   "printing.its_values_are_not_listed", "run x", 0),
        SL_DENY_AT("a-standard-module-holds-this-one", "src/collecting.ml",
                   "collecting.a_standard_module_holds_this_one", "Memprof.start x", 0),
    };
    ASSERT(sl_check("ocaml-functors", ".ml", "let filler x = x\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* A function's module is part of its name. */
static const char ML3_ARCHIVE[] = "module Name = struct\n"
                                  "  let dll_file x = x\n"
                                  "  let in_name x = dll_file x\n"
                                  "  module Deep = struct\n"
                                  "    let dll_file x = x\n"
                                  "    let in_deep x = dll_file x\n"
                                  "    let up_to_a_module x = in_name x\n"
                                  "  end\n"
                                  "end\n"
                                  "\n"
                                  "module Make (X : sig end) = struct\n"
                                  "  let dll_file x = x\n"
                                  "  let in_functor x = dll_file x\n"
                                  "end\n"
                                  "\n"
                                  "let dll_file x = Name.dll_file x\n"
                                  "let at_the_top x = dll_file x\n"
                                  "let through_two_modules x = Name.Deep.dll_file x\n"
                                  "module Applied = Make (struct end)\n"
                                  "let through_a_functor_application x = Applied.dll_file x\n"
                                  "let no_such_member_of_a_result x = Applied.not_there x\n";
static const char ML3_USER[] = "let qualified x = Archive.Name.dll_file x\n"
                               "let top_of_the_unit x = Archive.dll_file x\n";
#define ML3_FILE "src/archive.ml"
#define ML3_CALL(id, caller, text, at, tsuffix, line, decl)                                    \
    SL_CALL_AT(id, ML3_FILE, caller, text, at, ML3_FILE, tsuffix, "Function", line, decl)

TEST(resolver_source_ocaml_a_module_is_part_of_a_functions_name) {
    static const RQ_Source sources[] = {
        {.path = ML3_FILE, .text = ML3_ARCHIVE},
        {.path = "src/user.ml", .text = ML3_USER},
    };
    static const RQ_Expected expected[] = {
        ML3_CALL("own-module", "Name.in_name", "dll_file x", 1, "archive.Name.dll_file", 2,
                 "  let dll_file x = x"),
        ML3_CALL("inner-module-shadows-its-parent", "Deep.in_deep", "dll_file x", 3,
                 "Name.Deep.dll_file", 5, "    let dll_file x = x"),
        ML3_CALL("enclosing-module-from-a-nested-one", "Deep.up_to_a_module", "in_name x", 1,
                 "archive.Name.in_name", 3, "  let in_name x = dll_file x"),
        ML3_CALL("functor-body-reads-its-own", "Make.in_functor", "dll_file x", 5,
                 "archive.Make.dll_file", 12, "  let dll_file x = x"),
        ML3_CALL("the-unit-reads-a-nested-module", "src.archive.dll_file", "Name.dll_file x", 0,
                 "archive.Name.dll_file", 2, "  let dll_file x = x"),
        ML3_CALL("top-level-let-of-the-same-name", "src.archive.at_the_top", "dll_file x", 8,
                 "src.archive.dll_file", 16, "let dll_file x = Name.dll_file x"),
        ML3_CALL("two-modules-down", "src.archive.through_two_modules", "Name.Deep.dll_file x", 0,
                 "Name.Deep.dll_file", 5, "    let dll_file x = x"),
        ML3_CALL("a-functor-result-holds-what-its-struct-holds",
                 "src.archive.through_a_functor_application", "Applied.dll_file x", 0,
                 "archive.Make.dll_file", 12, "  let dll_file x = x"),
        SL_CALL_AT("from-another-unit", "src/user.ml", "user.qualified", "Archive.Name.dll_file x",
                   0, ML3_FILE, "archive.Name.dll_file", "Function", 2, "  let dll_file x = x"),
        SL_CALL_AT("the-units-own-from-another-unit", "src/user.ml", "user.top_of_the_unit",
                   "Archive.dll_file x", 0, ML3_FILE, "src.archive.dll_file", "Function", 16,
                   "let dll_file x = Name.dll_file x"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("a-functor-result-holds-nothing-else", ML3_FILE,
                   "archive.no_such_member_of_a_result", "Applied.not_there x", 0),
    };
    ASSERT(sl_check("ocaml-module-names", ".ml", "let filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Haskell: imports by header, constructors ──────────────────────── */

static const char HS_TYPES[] =
    "module Lib.Types\n"
    "  ( Open(..)\n"
    "  , Abstract\n"
    "  , mk\n"
    "  ) where\n"
    "\n"
    "data Open = Open Int | Other Int\n"
    "\n"
    "data Abstract = Abstract Int\n"
    "\n"
    "mk :: Int -> Abstract\n"
    "mk = Abstract\n";
static const char HS_USER[] =
    "module Lib.User where\n"
    "\n"
    "import Lib.Types (Open(..), Abstract, mk)\n"
    "import Lib.Missing\n"
    "\n"
    "data Local = Local Int | Renamed Int\n"
    "\n"
    "ownConstructor :: Int -> Local\n"
    "ownConstructor x = Local x\n"
    "\n"
    "importedConstructor :: Int -> Open\n"
    "importedConstructor x = Open x\n"
    "\n"
    "noConstructorWithoutNode :: Int -> Local\n"
    "noConstructorWithoutNode x = Renamed x\n"
    "\n"
    "noOtherConstructor :: Int -> Open\n"
    "noOtherConstructor x = Other x\n"
    "\n"
    "noPatternIsACall :: Local -> Int\n"
    "noPatternIsACall (Local x) = x\n"
    "noPatternIsACall (Renamed x) = x\n"
    "\n"
    "noCaseIsACall :: Local -> Int\n"
    "noCaseIsACall v = case v of\n"
    "  Local x -> x\n"
    "  Renamed x -> x\n";

TEST(resolver_source_haskell_constructors) {
    static const RQ_Source sources[] = {{.path = "src/Lib/Types.hs", .text = HS_TYPES},
                                        {.path = "src/Lib/User.hs", .text = HS_USER}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("constructor-named-as-its-type", "src/Lib/User.hs", "User.ownConstructor", "Local x", 0, "src/Lib/User.hs", "User.Local", "Class", 6,
                   "data Local = Local Int | Renamed Int"),
        SL_CALL_AT("imported-constructor-listed-with-children", "src/Lib/User.hs", "User.importedConstructor", "Open x", 0, "src/Lib/Types.hs", "Types.Open", "Class", 7,
                   "data Open = Open Int | Other Int"),
        {.case_id = "import-names-the-module-its-header-declares",
         .site = {.file = "src/Lib/User.hs", .caller = "User.hs.__file__", .needle = "Lib.Types"},
         .relation = "IMPORTS",
         .target = {.file = "src/Lib/Types.hs",
                    .suffix = "Types.hs.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "module Lib.Types"}},
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("constructor-with-no-node", "src/Lib/User.hs", "User.noConstructorWithoutNode", "Renamed x", 0),
        SL_DENY_AT("other-constructor-with-no-node", "src/Lib/User.hs", "User.noOtherConstructor", "Other x", 0),
        SL_DENY_AT("function-pattern-matches", "src/Lib/User.hs", "User.noPatternIsACall", "Local x", 1),
        SL_DENY_AT("case-pattern-matches", "src/Lib/User.hs", "User.noCaseIsACall", "Local x", 2),
    };
    ASSERT(sl_check("haskell-constructors", ".hs", "module Filler where\nf = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

static const char HS_KINDS[] = "module Lib.Kinds\n"
                               "  ( Kind(..)\n"
                               "  , Partial(one)\n"
                               "  , Hidden\n"
                               "  , localUse\n"
                               "  ) where\n"
                               "\n"
                               "class Kind a where\n"
                               "  status :: a -> Int\n"
                               "  code, level :: a -> Int\n"
                               "  level x = 1\n"
                               "\n"
                               "class Partial a where\n"
                               "  one :: a -> Int\n"
                               "  two :: a -> Int\n"
                               "\n"
                               "class Hidden a where\n"
                               "  hid :: a -> Int\n"
                               "\n"
                               "localUse :: Kind a => a -> Int\n"
                               "localUse x = status x\n"
                               "\n"
                               "instance Kind Int where\n"
                               "  status x = x\n"
                               "  code x = x\n"
                               "\n"
                               "instance Partial Int where\n"
                               "  one x = x\n"
                               "  two x = x\n"
                               "\n"
                               "instance Hidden Int where\n"
                               "  hid x = x\n";
static const char HS_USE_KINDS[] = "module Lib.UseKinds where\n"
                                   "\n"
                                   "import Lib.Kinds (Kind(..), Partial(one))\n"
                                   "\n"
                                   "viaAll x = status x\n"
                                   "viaAllBinding x = level x\n"
                                   "viaListed x = one x\n"
                                   "noNotListed x = two x\n"
                                   "noHidden x = hid x\n";
static const char HS_WHOLE[] = "module Lib.Whole where\n"
                               "\n"
                               "import Lib.Kinds\n"
                               "\n"
                               "viaWhole x = code x\n"
                               "noPartialChild x = two x\n"
                               "noHiddenWhole x = hid x\n";
#define HS_KINDS_FILE "src/Lib/Kinds.hs"
#define HS_METHOD(id, file, caller, text, method, line, decl)                                  \
    SL_CALL_AT(id, file, caller, text, 0, HS_KINDS_FILE, "Kinds." method, "Function", line, decl)

TEST(resolver_source_haskell_class_methods) {
    static const RQ_Source sources[] = {{.path = HS_KINDS_FILE, .text = HS_KINDS},
                                        {.path = "src/Lib/UseKinds.hs", .text = HS_USE_KINDS},
                                        {.path = "src/Lib/Whole.hs", .text = HS_WHOLE}};
    static const RQ_Expected expected[] = {
        HS_METHOD("method-in-its-own-module", HS_KINDS_FILE, "Kinds.localUse", "status x", "status",
                  24, "  status x = x"),
        HS_METHOD("method-through-children-all", "src/Lib/UseKinds.hs", "UseKinds.viaAll",
                  "status x", "status", 24, "  status x = x"),
        HS_METHOD("method-of-a-binding-list", "src/Lib/UseKinds.hs", "UseKinds.viaAllBinding",
                  "level x", "level", 11, "  level x = 1"),
        HS_METHOD("method-listed-by-name", "src/Lib/UseKinds.hs", "UseKinds.viaListed", "one x",
                  "one", 28, "  one x = x"),
        HS_METHOD("method-through-a-whole-import", "src/Lib/Whole.hs", "Whole.viaWhole", "code x",
                  "code", 25, "  code x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("method-not-in-the-import-list", "src/Lib/UseKinds.hs", "UseKinds.noNotListed",
                   "two x", 0),
        SL_DENY_AT("class-exported-without-children", "src/Lib/UseKinds.hs", "UseKinds.noHidden",
                   "hid x", 0),
        SL_DENY_AT("method-not-in-the-export-list", "src/Lib/Whole.hs", "Whole.noPartialChild",
                   "two x", 0),
        SL_DENY_AT("hidden-method-with-a-whole-import", "src/Lib/Whole.hs", "Whole.noHiddenWhole",
                   "hid x", 0),
    };
    ASSERT(sl_check("haskell-class-methods", ".hs", "module Filler where\nf = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── A grammar's rejection stays where it is ───────────────────────── */

/* `@Inject() (...)` is the shape tree-sitter-scala rejects most in playframework: the
 * rejection is in the class header, so the rest of the file still decides. */
static const char SC_DAMAGED[] =
    "package demo\n"
    "\n"
    "class Damaged @Inject() (a: Int)(implicit b: Long) {\n"
    "  def clean(): Int = helper()\n"
    "  def helper(): Int = 1\n"
    "  def damaged(): Int = {\n"
    "    val = 1\n"
    "    helper()\n"
    "  }\n"
    "}\n";
/* A stray closer moves everything after it to another scope. */
static const char SC_UNBALANCED[] =
    "package demo\n"
    "\n"
    "class Unbalanced {\n"
    "  def one(): Int = two()\n"
    "  def two(): Int = 1\n"
    "  }\n"
    "}\n"
    "class After {\n"
    "  def three(): Int = four()\n"
    "  def four(): Int = 1\n"
    "}\n";

TEST(resolver_source_parse_damage_stays_where_it_is) {
    static const RQ_Source sources[] = {{.path = "src/Damaged.scala", .text = SC_DAMAGED},
                                        {.path = "src/Unbalanced.scala", .text = SC_UNBALANCED}};
    static const RQ_Expected expected[] = {
        SL_CALL_AT("clean-method-of-a-damaged-file-decides", "src/Damaged.scala", "Damaged.clean",
                   "helper()", 0, "src/Damaged.scala", "Damaged.helper", "Method", 5,
                   "  def helper(): Int = 1"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("damaged-method-decides-nothing", "src/Damaged.scala", "Damaged.damaged",
                   "helper()", 2),
        SL_DENY_AT("unbalanced-file-decides-nothing", "src/Unbalanced.scala", "After.three", "four()",
                   0),
    };
    ASSERT(sl_check("parse-damage", ".scala", "package filler\nobject F { def f(): Int = 1 }\n",
                    sources, sizeof(sources) / sizeof(sources[0]), expected,
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
    "void topLocal() {}\n"
    "class Chains {\n"
    "  final Extra extra = Extra();\n"
    "  Extra give() => extra;\n"
    "  void callsDeep() { give().deep(); }\n"
    "  void callsCascade() { extra..casc(); }\n"
    "  void mentions() { final f = topLocal; f(); }\n"
    "  void noMemberName(Extra e) { e.topLocal; }\n"
    "}\n";
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
                                 "void hiddenName() {}\n"
                                 "class Extra {\n"
                                 "  void deep() {}\n"
                                 "  void casc() {}\n"
                                 "}\n";
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
#define DART_STORE_CLASS_AT(id, method, text, at)                                              \
    SL_CALL_AT(id, DART_FILE, "Counter." method, text, at, DART_STORE_FILE, "store.Store", "Class",\
               1, "class Store {")
#define DART_STORE_CLASS(id, method, text) DART_STORE_CLASS_AT(id, method, text, 0)

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
        DART_CALL("mixin-member", "callsMixin", "fromMix()", DART_BASE_FILE, "Mix.fromMix", "Method",
                  6, "  void fromMix() {}"),
        DART_CALL("last-mixin-wins", "callsLastMixin", "both()", DART_BASE_FILE, "Later.both",
                  "Method", 10, "  void both() {}"),
        DART_CALL("library-function-beats-inherited-member", "callsLibraryFirst", "fromLibrary()",
                  DART_FILE, "main.fromLibrary", "Function", 37, "void fromLibrary() {}"),
        DART_SAVE("declared-field-receiver", "callsField", "store.save"),
        DART_SAVE("field-initialized-by-constructor", "callsInferred", "made.save"),
        DART_SAVE("declared-parameter-receiver", "callsParam", "p.save"),
        DART_STORE_CLASS_AT("constructor-call", "callsLocal", "Store()", 1),
        DART_SAVE("local-initialized-by-constructor", "callsLocal", "l.save"),
        DART_STORE_CLASS("named-constructor", "callsTyped", "Store.named"),
        DART_SAVE("declared-local-receiver", "callsTyped", "t.save"),
        DART_CALL("static-method-on-class", "callsStatic", "Store.make", DART_STORE_FILE,
                  "Store.make", "Method", 4, "  static Store make() => Store();"),
        DART_CALL("library-top-level", "callsTop", "topLocal()", DART_FILE, "main.topLocal",
                  "Function", 38, "void topLocal() {}"),
        DART_CALL("through-package-uri-and-export", "callsBarrel", "helper()", DART_STORE_FILE,
                  "store.helper", "Function", 7, "void helper() {}"),
        DART_CALL("prefix-with-show", "callsPrefix", "s.helper", DART_STORE_FILE, "store.helper",
                  "Function", 7, "void helper() {}"),
        DART_CALL("function-in-a-part", "callsPart", "fromPart()", "lib/main_part.dart",
                  "main_part.fromPart", "Function", 2, "void fromPart() {}"),
        DART_STORE_CLASS("new-expression", "callsNew", "new Store()"),
        SL_CALL_AT("part-sees-its-library", "lib/main_part.dart", "main_part.partCalls",
                   "topLocal()", 0, DART_FILE, "main.topLocal", "Function", 38,
                   "void topLocal() {}"),
        SL_CALL_AT("function-with-a-declared-return-type", DART_FILE, "Chains.callsDeep", "give()",
                   1, DART_FILE, "Chains.give", "Method", 41, "  Extra give() => extra;"),
        SL_CALL_AT("member-deep-in-a-selector-chain", DART_FILE, "Chains.callsDeep", ".deep()", 0,
                   DART_STORE_FILE, "Extra.deep", "Method", 12, "  void deep() {}"),
        SL_CALL_AT("cascade-selects-on-its-receiver", DART_FILE, "Chains.callsCascade", "..casc()",
                   0, DART_STORE_FILE, "Extra.casc", "Method", 13, "  void casc() {}"),
        SL_USE_AT("declared-type-of-a-local", DART_FILE, "Counter.callsTyped", "Store", 4,
                  DART_STORE_FILE, "store.Store", "Class", 1, "class Store {"),
        SL_USE_AT("head-of-a-named-constructor-is-read", DART_FILE, "Counter.callsTyped", "Store", 5,
                  DART_STORE_FILE, "store.Store", "Class", 1, "class Store {"),
        SL_USE_AT("function-read-as-a-value", DART_FILE, "Chains.mentions", "topLocal", 4,
                  DART_FILE, "main.topLocal", "Function", 38, "void topLocal() {}"),
        SL_CALL_AT("own-member-under-an-outside-superclass", "lib/ext.dart", "W.callsOwnW",
                   "ownW()", 0, "lib/ext.dart", "W.ownW", "Method", 9, "  void ownW() {}"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("member-name-after-a-dot-is-not-a-read", DART_FILE, "Chains.noMemberName",
                     "topLocal", 5),
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
    "    func noImplicitMember() { use(.ownM) }\n"
    "}\n"
    "struct Other { func put() {} }\n"
    "class Outside: NSObject {\n"
    "    func own() {}\n"
    "    func callsOwnMember() { own() }\n"
    "    func noFreeUnderOpenBase() { sameFile() }\n"
    "    func noInherited() { inheritedFromOutside() }\n"
    "    func usesTypeName() -> Any { return Box.self }\n"
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
                SW_FILE, "Other.put", "Method", 55, "struct Other { func put() {} }"),
        SW_CALL("initializer-of-the-shadowing-local", "localShadowsProperty", "Other()", 0, SW_FILE,
                "Main.Other", "Struct", 55, "struct Other { func put() {} }"),
        SL_CALL_AT("own-member-under-an-outside-superclass", SW_FILE, "Outside.callsOwnMember",
                   "own()", 1, SW_FILE, "Outside.own", "Method", 57, "    func own() {}"),
        SL_USE_AT("type-read-as-a-value", SW_FILE, "Outside.usesTypeName", "Box", 11, SW_FILE,
                  "Main.Box", "Struct", 6, "struct Box {"),
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
        {.case_id = "implicit-member-is-not-a-name-in-scope",
         .site = {.file = SW_FILE,
                  .caller = "Service.noImplicitMember",
                  .needle = "ownM",
                  .occurrence_index = 7},
         .relation = "USAGE",
         .any_target = true},
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

/* Forms from Alamofire and Vapor. Without the parse view each of these files is voided. */
static const char SW_COND[] =
    "class Conditional {\n"                                                     /* 1 */
    "    #if os(iOS)\n"                                                         /* 2 */
    "    private let lock = LockA()\n"                                          /* 3 */
    "    func inBranch() {}\n"                                                  /* 4 */
    "    #elseif os(macOS)\n"                                                   /* 5 */
    "    private let lock = LockB()\n"                                          /* 6 */
    "    func otherBranch() {}\n"                                               /* 7 */
    "    #else\n"                                                               /* 8 */
    "    #error(\n"                                                              /* 9 */
    "      \"no lock\"\n"                                                        /* 10 */
    "    )\n"                                                                    /* 11 */
    "    #endif\n"                                                               /* 12 */
    "    private nonisolated(unsafe) var state: Holder = Holder()\n"            /* 13 */
    "    func callsBranches() {\n"                                              /* 14 */
    "        inBranch()\n"                                                      /* 15 */
    "        otherBranch()\n"                                                   /* 16 */
    "    }\n"                                                                   /* 17 */
    "    func unitArgument() -> Holder { use(.success(())); return state }\n"   /* 18 */
    "    func consumes(h: Holder) { let k = consume h; k.go() }\n"              /* 19 */
    "    func typedThrows() throws(MyError) { state.go() }\n"                   /* 20 */
    "    func unsafeForm(h: Holder) { unsafe h.go() }\n"                        /* 21 */
    "    func sends(h: sending Holder) { h.go() }\n"                            /* 22 */
    "    func labelNamedCopy(copy h: Holder) { h.go() }\n"                      /* 23 */
    "    func callsLabel(h: Holder) { labelNamedCopy(copy: h) }\n"              /* 24 */
    "    func noTwoBranchTypes() { lock.go() }\n"                               /* 25 */
    "    init(seed: Holder) { seed.go() }\n"                                    /* 26 */
    "    deinit { state.go() }\n"                                               /* 27 */
    "    subscript(i: Int) -> Int { state.go(); return i }\n"                   /* 28 */
    "}\n"                                                                       /* 29 */
    "struct Holder { func go() {} }\n"                                          /* 30 */
    "struct LockA { func go() {} }\n"                                           /* 31 */
    "struct LockB { func go() {} }\n";                                          /* 32 */
static const char SW_INITS[] =
    "class Parent {\n"                                                          /* 1 */
    "    init(a: Int) {}\n"                                                     /* 2 */
    "}\n"                                                                       /* 3 */
    "class Child: Parent {\n"                                                   /* 4 */
    "    override init(a: Int) { super.init(a: a) }\n"                          /* 5 */
    "    func noInitOfAValue(c: Child) { c.init(z: 1) }\n"                      /* 6 */
    "    func madeByType() -> Child { Child(a: 1) }\n"                          /* 7 */
    "}\n";                                                                      /* 8 */
static const char SW_READS[] =
    "import Lib\n"                                                              /* 1 */
    "enum Mode { case fast, slow(reason: String) }\n"                           /* 2 */
    "class Settings: Base {\n"                                                  /* 3 */
    "    static let shared = Settings()\n"                                      /* 4 */
    "    let timeout = 10\n"                                                    /* 5 */
    "    var mode: Mode = .fast\n"                                              /* 6 */
    "    func annotated() { let m: Mode? = nil; use(m) }\n"                     /* 7 */
    "    func generic() { var xs: [Int: Mode] = [:]; xs.removeAll() }\n"        /* 8 */
    "    func bareRead() { use(timeout) }\n"                                    /* 9 */
    "    func selfRead() { use(self.timeout) }\n"                               /* 10 */
    "    func memberRead(s: Settings) { use(s.timeout) }\n"                     /* 11 */
    "    func staticRead() { use(Settings.shared) }\n"                          /* 12 */
    "    func enumRead() { use(Mode.fast) }\n"                                  /* 13 */
    "    func keyPathRead() { use(\\Settings.timeout) }\n"                      /* 14 */
    "    func noLabelIsARead() { take(timeout: 1) }\n"                          /* 15 */
    "    func noTupleLabel() { use((timeout: 1, x: 2)) }\n"                     /* 16 */
    "    func noParameter(timeout: Int) { use(timeout) }\n"                     /* 17 */
    "    func noLocal() { let timeout = 1; use(timeout) }\n"                    /* 18 */
    "    func noUnknownReceiver(x: Any) { use(x.timeout) }\n"                   /* 19 */
    "    func noImplicitMember() { use(.timeout) }\n"                           /* 20 */
    "    func noLocalType() { struct Holder {}; use(Holder()) }\n"              /* 21 */
    "    func noCalledMember(s: Settings) { s.bareRead() }\n"                   /* 22 */
    "    func nilDefault(a: Int, b: Settings? = nil) {}\n"                      /* 23 */
    "    func callsNilDefault() { nilDefault(a: 1) }\n"                         /* 24 */
    "    weak var monitor: (any Greeting)?\n"                                   /* 25 */
    "    func optionalExistential() { monitor?.greet() }\n"                     /* 26 */
    "}\n"                                                                       /* 27 */
    "protocol Greeting { func greet() }\n"                                      /* 28 */
    "struct Holder { let timeout = 1 }\n"                                       /* 29 */
    "struct Aliased { typealias Holder = Int; func f() { use(Holder.self) } }\n"; /* 30 */
static const char SW_EXT_LIB[] = "public class Session {\n"
                                 "    public init() {}\n"
                                 "    public func request(_ url: String) -> Req { Req() }\n"
                                 "    public func only() {}\n"
                                 "}\n"
                                 "public class Req {\n"
                                 "    public init() {}\n"
                                 "    public var session: Session { Session() }\n"
                                 "    public func validate() -> Req { self }\n"
                                 "}\n"
                                 "public extension Session {\n"
                                 "    var shadowed: Int { 1 }\n"
                                 "    var typedTwice: Int { 1 }\n"
                                 "    var onlyThere: Int { 1 }\n"
                                 "}\n";
static const char SW_EXT_APP[] =
    "import Lib\n"                                                              /* 1 */
    "extension Session {\n"                                                     /* 2 */
    "    func request(_ endpoint: Endpoint) -> Req { only(); return Req() }\n"  /* 3 */
    "    func added() {}\n"                                                     /* 4 */
    "}\n"                                                                       /* 5 */
    "struct Endpoint {}\n"                                                      /* 6 */
    "class Uses {\n"                                                            /* 7 */
    "    func exactTypeSelects(s: Session) { s.request(Endpoint()) }\n"         /* 8 */
    "    func addedHere(s: Session) { s.added() }\n"                            /* 9 */
    "    func declaredThere(s: Session) { s.only() }\n"                         /* 10 */
    "    func noSharedLabels(s: Session) { let e = unknownFn(); s.request(e) }\n" /* 11 */
    "}\n"                                                                       /* 12 */
    "extension Session {\n"                                                     /* 13 */
    "    var shadowed: Int { 2 }\n"                                             /* 14 */
    "    var typedTwice: String { \"\" }\n"                                     /* 15 */
    "}\n"                                                                       /* 16 */
    "class Shadows {\n"                                                         /* 17 */
    "    func readsLocal(r: Req) { use(r.session.shadowed) }\n"                 /* 18 */
    "    func readsImported(r: Req) { use(r.session.onlyThere) }\n"             /* 19 */
    "    func noTwoTypes(r: Req) { use(r.session.typedTwice) }\n"               /* 20 */
    "}\n";                                                                      /* 21 */

/* `try await` before a body's brace, which the grammar reads as a trailing closure. */
static const char SW_ASYNC[] =
    "struct Job { func run() {} }\n"                                            /* 1 */
    "func load() async throws -> Job? { nil }\n"                                /* 2 */
    "class Asyncs {\n"                                                          /* 3 */
    "    func ifTryAwait() async throws {\n"                                    /* 4 */
    "        if let j = try await load() {\n"                                   /* 5 */
    "            j.run()\n"                                                     /* 6 */
    "        }\n"                                                               /* 7 */
    "    }\n"                                                                   /* 8 */
    "    func elseBranch() async throws {\n"                                    /* 9 */
    "        if let j = try? await load() {\n"                                  /* 10 */
    "            j.run()\n"                                                     /* 11 */
    "        } else {\n"                                                        /* 12 */
    "            helper()\n"                                                    /* 13 */
    "        }\n"                                                               /* 14 */
    "    }\n"                                                                   /* 15 */
    "    func switchTryAwait() async throws {\n"                                /* 16 */
    "        switch try await load() {\n"                                       /* 17 */
    "        default: helper()\n"                                               /* 18 */
    "        }\n"                                                               /* 19 */
    "    }\n"                                                                   /* 20 */
    "    func awaitAsALabel(await x: Job) { x.run() }\n"                        /* 21 */
    "    func helper() {}\n"                                                    /* 22 */
    "}\n";                                                                      /* 23 */
#define SW_ASYNC_FILE "Sources/App/Async.swift"
#define SW_ASYNC_CALL(id, method, needle, at, suffix, label, line, decl)                       \
    SL_CALL_AT(id, SW_ASYNC_FILE, "Asyncs." method, needle, at, SW_ASYNC_FILE, suffix, label, line, decl)
#define SW_ASYNC_LOAD "func load() async throws -> Job? { nil }"
#define SW_ASYNC_RUN "struct Job { func run() {} }"
#define SW_ASYNC_HELPER "    func helper() {}"

/* An import names a target of the manifest. A directory of that name is not one. */
static const char SW_IMPORTS[] = "@testable import Lib\n"
                                 "public import class Lib.Session\n"
                                 "import Foundation\n"
                                 "import NotATarget\n";
#define SW_IMPORTS_FILE "Sources/App/Imports.swift"
#define SW_IMPORT_OF_LIB(id, text)                                                             \
    {                                                                                          \
        .case_id = id, .site = {.file = SW_IMPORTS_FILE, .caller = "Imports.swift.__file__",   \
                                .needle = text},                                               \
        .relation = "IMPORTS",                                                                 \
        .target = {.domain = RQ_TARGET_CONFIGURED_FOLDER,                                      \
                   .file = "Sources/Lib",                                                      \
                   .suffix = "Sources.Lib",                                                    \
                   .label = "Folder",                                                          \
                   .declaration_line = 5,                                                      \
                   .declaration_text = "    .target(name: \"Lib\", path: \"Sources/Lib\"),",     \
                   .configuration_file = "Package.swift"}                                      \
    }
#define SW_NO_IMPORT(id, text)                                                                 \
    {                                                                                          \
        .case_id = id, .site = {.file = SW_IMPORTS_FILE, .caller = "Imports.swift.__file__",   \
                                .needle = text},                                               \
        .relation = "IMPORTS", .any_target = true                                              \
    }

#define SW_COND_FILE "Sources/App/Cond.swift"
#define SW_READS_FILE "Sources/App/Reads.swift"
#define SW_EXT_LIB_FILE "Sources/Lib/Session.swift"
#define SW_EXT_APP_FILE "Sources/App/Ext.swift"
#define SW_GO_AT(id, caller, text, at, line, decl)                                             \
    SL_CALL_AT(id, SW_COND_FILE, caller, text, at, SW_COND_FILE, "Holder.go", "Method", line, decl)
#define SW_HOLDER_GO "struct Holder { func go() {} }"
#define SW_TYPE_AT(id, sfile, scaller, text, at, tfile, tsuffix, tlabel, tline, tdecl)         \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "USES_TYPE",                                                               \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = tlabel,                                                            \
                   .declaration_line = tline,                                                  \
                   .declaration_text = tdecl}                                                  \
    }
#define SW_READ_OF_SESSION(id, caller, at)                                                     \
    SL_USE_AT(id, SW_EXT_APP_FILE, caller, "session", at, SW_EXT_LIB_FILE, "Req.session", "Variable", \
              8, "    public var session: Session { Session() }")
#define SW_READ(id, method, needle, at, suffix, label, line, decl)                             \
    SL_USE_AT(id, SW_READS_FILE, "Settings." method, needle, at, SW_READS_FILE, suffix, label, line, \
              decl)

TEST(resolver_source_swift_parse_view_reads_and_extensions) {
    static const RQ_Source sources[] = {
        {.path = "Package.swift", .text = SW_MANIFEST},
        {.path = SW_COND_FILE, .text = SW_COND},
        {.path = SW_READS_FILE, .text = SW_READS},
        {.path = SW_EXT_LIB_FILE, .text = SW_EXT_LIB},
        {.path = SW_EXT_APP_FILE, .text = SW_EXT_APP},
        {.path = "Sources/App/Base.swift", .text = "class Base {}\n"},
        {.path = "Sources/App/Inits.swift", .text = SW_INITS},
        {.path = SW_ASYNC_FILE, .text = SW_ASYNC},
        {.path = SW_IMPORTS_FILE, .text = SW_IMPORTS},
        {.path = "Sources/Foundation/Decoy.swift", .text = "func decoy() {}\n"},
        {.path = "Scripts/imports.swift", .text = "import Lib\n"},
        {.path = "Other/Package.swift",
         .text = "let package = Package(name: \"Other\", targets: [.target(name: \"Other\")])\n"},
        {.path = "Other/Sources/Other/Uses.swift", .text = "import Lib\n"},
    };
    static const RQ_Expected expected[] = {
        SW_IMPORT_OF_LIB("import-of-a-target-reaches-its-directory", "@testable import Lib"),
        SW_IMPORT_OF_LIB("import-of-a-member-names-the-module-first",
                         "public import class Lib.Session"),
        SW_ASYNC_CALL("try-await-before-a-body", "ifTryAwait", "load()", 1, "Async.load", "Function", 2,
                      SW_ASYNC_LOAD),
        SW_ASYNC_CALL("the-body-after-try-await", "ifTryAwait", "j.run()", 0, "Job.run", "Method", 1,
                      SW_ASYNC_RUN),
        SW_ASYNC_CALL("try-optional-await-before-a-body", "elseBranch", "load()", 2, "Async.load",
                      "Function", 2, SW_ASYNC_LOAD),
        SW_ASYNC_CALL("the-body-after-try-optional-await", "elseBranch", "j.run()", 1, "Job.run",
                      "Method", 1, SW_ASYNC_RUN),
        SW_ASYNC_CALL("the-else-after-try-optional-await", "elseBranch", "helper()", 0, "Asyncs.helper",
                      "Method", 22, SW_ASYNC_HELPER),
        SW_ASYNC_CALL("switch-on-try-await", "switchTryAwait", "load()", 3, "Async.load", "Function", 2,
                      SW_ASYNC_LOAD),
        SW_ASYNC_CALL("a-case-after-switch-on-try-await", "switchTryAwait", "helper()", 1,
                      "Asyncs.helper", "Method", 22, SW_ASYNC_HELPER),
        SW_ASYNC_CALL("a-label-named-await-stays-a-label", "awaitAsALabel", "x.run()", 0, "Job.run",
                      "Method", 1, SW_ASYNC_RUN),
        SL_CALL_AT("member-in-an-if-branch-of-a-type-body", SW_COND_FILE,
                   "Conditional.callsBranches", "inBranch()", 1, SW_COND_FILE,
                   "Conditional.inBranch", "Method", 4, "    func inBranch() {}"),
        SL_CALL_AT("member-in-another-branch-of-a-type-body", SW_COND_FILE,
                   "Conditional.callsBranches", "otherBranch()", 1, SW_COND_FILE,
                   "Conditional.otherBranch", "Method", 7, "    func otherBranch() {}"),
        SW_GO_AT("unit-value-does-not-void-the-file", "Conditional.consumes", "k.go()", 0, 30,
                 SW_HOLDER_GO),
        SW_GO_AT("typed-throws", "Conditional.typedThrows", "state.go()", 0, 30, SW_HOLDER_GO),
        SW_GO_AT("unsafe-expression", "Conditional.unsafeForm", "h.go()", 0, 30, SW_HOLDER_GO),
        SW_GO_AT("sending-parameter", "Conditional.sends", "h.go()", 1, 30, SW_HOLDER_GO),
        SW_GO_AT("a-label-named-copy-stays-a-label", "Conditional.labelNamedCopy", "h.go()", 2, 30,
                 SW_HOLDER_GO),
        SL_CALL_AT("label-copy-selects-its-function", SW_COND_FILE, "Conditional.callsLabel",
                   "labelNamedCopy(copy: h)", 0, SW_COND_FILE, "Conditional.labelNamedCopy",
                   "Method", 23, "    func labelNamedCopy(copy h: Holder) { h.go() }"),
        SW_GO_AT("call-inside-init-belongs-to-init", "Conditional.init", "seed.go()", 0, 30,
                 SW_HOLDER_GO),
        SW_GO_AT("call-inside-deinit-belongs-to-deinit", "Conditional.deinit", "state.go()", 1, 30,
                 SW_HOLDER_GO),
        SW_GO_AT("call-inside-subscript-belongs-to-subscript", "Conditional.subscript", "state.go()",
                 2, 30, SW_HOLDER_GO),
        SL_CALL_AT("super-init-selects-an-initializer", "Sources/App/Inits.swift", "Child.init",
                   "super.init(a: a)", 0, "Sources/App/Inits.swift", "Parent.init", "Method", 2,
                   "    init(a: Int) {}"),
        SL_CALL_AT("calling-the-type-stays-a-class-call", "Sources/App/Inits.swift",
                   "Child.madeByType", "Child(a: 1)", 0, "Sources/App/Inits.swift", "Inits.Child",
                   "Class", 4, "class Child: Parent {"),
        SL_USE_AT("the-return-type-beside-that-call", "Sources/App/Inits.swift", "Child.madeByType",
                  "Child", 2, "Sources/App/Inits.swift", "Inits.Child", "Class", 4,
                  "class Child: Parent {"),
        SW_TYPE_AT("that-return-type-is-a-use-of-the-type", "Sources/App/Inits.swift", "Child.madeByType",
                   "Child", 2, "Sources/App/Inits.swift", "Inits.Child", "Class", 4,
                   "class Child: Parent {"),
        SW_READ("type-annotation-of-a-local", "annotated", "Mode", 2, "Reads.Mode", "Class", 2,
                "enum Mode { case fast, slow(reason: String) }"),
        SW_READ("type-inside-a-generic-annotation", "generic", "Mode", 3, "Reads.Mode", "Class", 2,
                "enum Mode { case fast, slow(reason: String) }"),
        SW_READ("bare-name-of-a-property", "bareRead", "timeout", 1, "Settings.timeout", "Variable",
                5, "    let timeout = 10"),
        SW_READ("self-property", "selfRead", "timeout", 2, "Settings.timeout", "Variable", 5,
                "    let timeout = 10"),
        SW_READ("property-of-a-declared-receiver", "memberRead", "timeout", 3, "Settings.timeout",
                "Variable", 5, "    let timeout = 10"),
        SW_READ("key-path-with-a-written-root", "keyPathRead", "timeout", 4, "Settings.timeout",
                "Variable", 5, "    let timeout = 10"),
        SW_READ("the-root-of-that-key-path", "keyPathRead", "Settings", 4, "Reads.Settings", "Class",
                3, "class Settings: Base {"),
        SW_READ("static-member-read", "staticRead", "shared", 1, "Settings.shared", "Variable", 4,
                "    static let shared = Settings()"),
        SW_READ("the-type-read-for-that-member", "staticRead", "Settings", 3, "Reads.Settings",
                "Class", 3, "class Settings: Base {"),
        SW_READ("enum-case-read", "enumRead", "fast", 2, "Mode.fast", "Variable", 2,
                "enum Mode { case fast, slow(reason: String) }"),
        SW_READ("the-enum-read-for-that-case", "enumRead", "Mode", 4, "Reads.Mode", "Class", 2,
                "enum Mode { case fast, slow(reason: String) }"),
        SL_USE_AT("a-property-here-shadows-the-imported-one", SW_EXT_APP_FILE, "Shadows.readsLocal",
                  "shadowed", 1, SW_EXT_APP_FILE, "Ext.Session.shadowed", "Variable", 14,
                  "    var shadowed: Int { 2 }"),
        SW_READ_OF_SESSION("the-receiver-of-that-property", "Shadows.readsLocal", 0),
        SL_USE_AT("a-public-extension-makes-its-members-public", SW_EXT_APP_FILE, "Shadows.readsImported",
                  "onlyThere", 0, SW_EXT_LIB_FILE, "Session.Session.onlyThere", "Variable", 14,
                  "    var onlyThere: Int { 1 }"),
        SW_READ_OF_SESSION("the-receiver-of-the-imported-property", "Shadows.readsImported", 1),
        SW_READ_OF_SESSION("the-receiver-of-a-property-of-two-types", "Shadows.noTwoTypes", 2),
        SL_CALL_AT("exact-parameter-type-selects-across-modules", SW_EXT_APP_FILE,
                   "Uses.exactTypeSelects", "s.request(Endpoint())", 0, SW_EXT_APP_FILE,
                   "Ext.Session.request", "Method", 3,
                   "    func request(_ endpoint: Endpoint) -> Req { only(); return Req() }"),
        SL_CALL_AT("the-argument-of-that-call", SW_EXT_APP_FILE, "Uses.exactTypeSelects",
                   "Endpoint()", 0, SW_EXT_APP_FILE, "Ext.Endpoint", "Struct", 6,
                   "struct Endpoint {}"),
        SL_CALL_AT("the-other-call-in-that-extension", SW_EXT_APP_FILE, "Ext.Session.request",
                   "Req()", 0, SW_EXT_LIB_FILE, "Session.Req", "Class", 6, "public class Req {"),
        SL_USE_AT("the-return-type-of-that-extension-member", SW_EXT_APP_FILE, "Ext.Session.request",
                  "Req", 0, SW_EXT_LIB_FILE, "Session.Req", "Class", 6, "public class Req {"),
        SW_TYPE_AT("the-return-type-is-a-use-of-the-type", SW_EXT_APP_FILE, "Ext.Session.request", "Req",
                   0, SW_EXT_LIB_FILE, "Session.Req", "Class", 6, "public class Req {"),
        SL_CALL_AT("a-nil-default-makes-a-parameter-optional", SW_READS_FILE,
                   "Settings.callsNilDefault", "nilDefault(a: 1)", 0, SW_READS_FILE,
                   "Settings.nilDefault", "Method", 23,
                   "    func nilDefault(a: Int, b: Settings? = nil) {}"),
        SL_CALL_AT("optional-existential-receiver", SW_READS_FILE, "Settings.optionalExistential",
                   "monitor?.greet()", 0, SW_READS_FILE, "Greeting.greet", "Method", 28,
                   "protocol Greeting { func greet() }"),
        SL_CALL_AT("extension-in-this-module-of-an-imported-type", SW_EXT_APP_FILE, "Uses.addedHere",
                   "s.added()", 0, SW_EXT_APP_FILE, "Ext.Session.added", "Method", 4,
                   "    func added() {}"),
        SL_CALL_AT("member-the-imported-type-declares", SW_EXT_APP_FILE, "Uses.declaredThere",
                   "s.only()", 0, SW_EXT_LIB_FILE, "Session.only", "Method", 4,
                   "    public func only() {}"),
        SL_CALL_AT("bare-name-in-an-extension-of-an-imported-type", SW_EXT_APP_FILE,
                   "Ext.Session.request", "only()", 0, SW_EXT_LIB_FILE, "Session.only", "Method", 4,
                   "    public func only() {}"),
    };
    static const RQ_Forbidden forbidden[] = {
        SW_NO_IMPORT("a-directory-of-that-name-is-no-target", "import Foundation"),
        SW_NO_IMPORT("a-module-outside-the-manifest", "import NotATarget"),
        {.case_id = "a-file-of-no-target-proves-no-package",
         .site = {.file = "Scripts/imports.swift", .caller = "imports.swift.__file__", .needle = "import Lib"},
         .relation = "IMPORTS",
         .any_target = true},
        {.case_id = "another-manifest-needs-its-dependency-chain",
         .site = {.file = "Other/Sources/Other/Uses.swift",
                  .caller = "Uses.swift.__file__",
                  .needle = "import Lib"},
         .relation = "IMPORTS",
         .any_target = true},
        SL_DENY_AT("no-initializer-has-these-labels", "Sources/App/Inits.swift",
                   "Child.noInitOfAValue", "c.init(z: 1)", 0),
        SL_DENY_AT("branches-disagree-on-the-type", SW_COND_FILE, "Conditional.noTwoBranchTypes",
                   "lock.go()", 0),
        SL_NO_USE_AT("a-property-of-another-type-does-not-shadow", SW_EXT_APP_FILE, "Shadows.noTwoTypes",
                     "typedTwice", 1),
        SL_DENY_AT("overloads-across-modules-share-labels", SW_EXT_APP_FILE, "Uses.noSharedLabels",
                   "s.request(e)", 0),
        SL_NO_USE_AT("argument-label-is-not-a-read", SW_READS_FILE, "Settings.noLabelIsARead",
                     "timeout", 5),
        SL_NO_USE_AT("tuple-label-is-not-a-read", SW_READS_FILE, "Settings.noTupleLabel", "timeout",
                     6),
        SL_NO_USE_AT("parameter-shadows-property", SW_READS_FILE, "Settings.noParameter", "timeout",
                     8),
        SL_NO_USE_AT("local-shadows-property", SW_READS_FILE, "Settings.noLocal", "timeout", 10),
        SL_NO_USE_AT("unknown-receiver", SW_READS_FILE, "Settings.noUnknownReceiver", "timeout", 11),
        SL_NO_USE_AT("implicit-member-is-not-a-name-in-scope", SW_READS_FILE,
                     "Settings.noImplicitMember", "timeout", 12),
        SL_DENY_AT("local-type-hides-the-module-type", SW_READS_FILE, "Settings.noLocalType",
                   "Holder()", 0),
        SL_NO_USE_AT("called-member-is-not-a-read", SW_READS_FILE, "Settings.noCalledMember",
                     "bareRead", 1),
        SL_NO_USE_AT("alias-in-a-type-hides-the-module-type", SW_READS_FILE, "Aliased.f", "Holder",
                     4),
    };
    ASSERT(sl_check("swift-reads", ".swift", "func filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* Implicit members and overloads that share labels across files, as Alamofire's tests use them. */
static const char SW_CTX[] =
    "enum Mode {\n"                                                             /* 1 */
    "    case fast, none\n"                                                     /* 2 */
    "    case slow(reason: String)\n"                                           /* 3 */
    "    static let shared = Mode.fast\n"                                       /* 4 */
    "    static func make() -> Mode { .fast }\n"                                /* 5 */
    "    func label() {}\n"                                                     /* 6 */
    "}\n"                                                                       /* 7 */
    "class Alpha { static let onlyAlpha = Alpha(); static let both = Alpha() }\n" /* 8 */
    "class Omega: Alpha {}\n"                                                   /* 9 */
    "class Beta { static let both = Beta() }\n"                                 /* 10 */
    "protocol Proto { func required() }\n"                                      /* 11 */
    "struct Impl: Proto { func required() {} }\n"                               /* 12 */
    "struct Named: ExpressibleByStringLiteral { init(stringLiteral v: String) {} }\n" /* 13 */
    "func pick(_ a: Alpha) {}\n"                                                /* 14 */
    "func conv(_ p: any Proto) {}\n"                                            /* 15 */
    "func lit(_ n: Named) {}\n"                                                 /* 16 */
    "func out(_ a: Alpha) {}\n"                                                 /* 17 */
    "func takes(m: Mode) {}\n"                                                  /* 18 */
    "func optional(m: Mode?) {}\n"                                              /* 19 */
    "func closureLast(queue: Beta = .both, done: () -> Void) {}\n"              /* 20 */
    "\n"                                                                        /* 21 */
    "struct Point { let mode = 1; static func mode(of: Int) -> Point { Point() } }\n" /* 22 */
    "class Uses {\n"                                                            /* 23 */
    "    var current: Mode = .fast\n"                                           /* 24 */
    "    func annotated() { let m: Mode = .shared; use(m) }\n"                  /* 25 */
    "    func returned() -> Mode { return .shared }\n"                          /* 26 */
    "    func implicitlyReturned() -> Mode { .shared }\n"                       /* 27 */
    "    func compared(m: Mode) { if m == .fast { use(m) } }\n"                 /* 28 */
    "    func assigned() { current = .shared }\n"                               /* 29 */
    "    func switched(m: Mode) { switch m { case .fast: break; default: break } }\n" /* 30 */
    "    func withPayload(m: Mode) { switch m { case .slow(let fast): use(fast); default: break } }\n" /* 31 */
    "    func argument() { takes(m: .shared) }\n"                               /* 32 */
    "    func called() { let m: Mode = .make(); m.label() }\n"                  /* 33 */
    "    func payload() { let m: Mode = .slow(reason: \"x\"); use(m) }\n"       /* 34 */
    "    func noOptionalNone() { optional(m: .none) }\n"                        /* 35 */
    "    func noContext() { use(.shared) }\n"                                   /* 36 */
    "    func noMember() { let m: Mode = .other; use(m) }\n"                    /* 37 */
    "    func pickByMember() { pick(.onlyAlpha) }\n"                            /* 38 */
    "    func noPickBoth() { pick(.both) }\n"                                   /* 39 */
    "    func pickAlpha() { pick(Alpha()) }\n"                                  /* 40 */
    "    func pickBeta() { pick(Beta()) }\n"                                    /* 41 */
    "    func pickSubclass() { pick(Omega()) }\n"                               /* 42 */
    "    func noOutsideForEither(u: URL) { pick(u) }\n"                         /* 43 */
    "    func exactBeatsExistential() { conv(Impl()) }\n"                       /* 44 */
    "    func existentialTakesIt(p: any Proto) { conv(p) }\n"                   /* 45 */
    "    func literalTakesIt() { lit(\"x\") }\n"                                /* 46 */
    "    func outsideGoesToTheProtocol(u: URL) { out(u) }\n"                    /* 47 */
    "    func trailing() { closureLast { } }\n"                                 /* 48 */
    "    func functionBesideValue() { _ = Point.mode(of: 1) }\n"                /* 49 */
    "}\n";                                                                      /* 50 */
static const char SW_CTX2[] = "func pick(_ b: Beta) {}\n"          /* 1 */
                              "func conv(_ i: Impl) {}\n"          /* 2 */
                              "func lit(_ a: Alpha) {}\n"          /* 3 */
                              "func out(_ p: any Proto) {}\n"      /* 4 */
                              "func closureLast(other: Int, more: Int) {}\n"; /* 5 */

#define SW_CTX_FILE "Sources/App/Ctx.swift"
#define SW_CTX2_FILE "Sources/App/Ctx2.swift"
#define SW_CTX_USE(id, method, needle, at, suffix, line, decl)                                 \
    SL_USE_AT(id, SW_CTX_FILE, "Uses." method, needle, at, SW_CTX_FILE, suffix, "Variable", line, decl)
#define SW_CTX_TYPE_USE(id, method, at)                                                        \
    SL_USE_AT(id, SW_CTX_FILE, "Uses." method, "Mode", at, SW_CTX_FILE, "Ctx.Mode", "Class", 1,    \
              "enum Mode {")
#define SW_CTX_USES_TYPE(id, method, at)                                                        \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = SW_CTX_FILE,                                                          \
                 .caller = "Uses." method,                                                     \
                 .needle = "Mode",                                                             \
                 .occurrence_index = at},                                                      \
        .relation = "USES_TYPE",                                                               \
        .target = {.file = SW_CTX_FILE,                                                        \
                   .suffix = "Ctx.Mode",                                                       \
                   .label = "Class",                                                           \
                   .declaration_line = 1,                                                      \
                   .declaration_text = "enum Mode {"}                                          \
    }
#define SW_CTX_MADE(id, method, needle, at, suffix, label, line, decl)                         \
    SL_CALL_AT(id, SW_CTX_FILE, "Uses." method, needle, at, SW_CTX_FILE, suffix, label, line, decl)
#define SW_MODE_SHARED_DECL "    static let shared = Mode.fast"
#define SW_MODE_CASES "    case fast, none"
#define SW_CTX_CALL(id, method, needle, tfile, tsuffix, tline, tdecl)                          \
    SL_CALL_AT(id, SW_CTX_FILE, "Uses." method, needle, 0, tfile, tsuffix, "Function", tline, tdecl)

TEST(resolver_source_swift_implicit_members_and_overloads_by_type) {
    static const RQ_Source sources[] = {
        {.path = "Package.swift", .text = SW_MANIFEST},
        {.path = SW_CTX_FILE, .text = SW_CTX},
        {.path = SW_CTX2_FILE, .text = SW_CTX2},
    };
    static const RQ_Expected expected[] = {
        SW_CTX_USE("annotation-gives-the-context", "annotated", "shared", 1, "Mode.shared", 4,
                   SW_MODE_SHARED_DECL),
        SW_CTX_TYPE_USE("the-annotation-itself", "annotated", 6),
        SW_CTX_USE("return-type-gives-the-context", "returned", "shared", 2, "Mode.shared", 4,
                   SW_MODE_SHARED_DECL),
        SW_CTX_TYPE_USE("the-return-type-itself", "returned", 7),
        SW_CTX_USES_TYPE("the-return-type-is-a-use-of-the-type", "returned", 7),
        SW_CTX_USE("implicit-return-gives-the-context", "implicitlyReturned", "shared", 3,
                   "Mode.shared", 4, SW_MODE_SHARED_DECL),
        SW_CTX_TYPE_USE("the-implicit-return-type-itself", "implicitlyReturned", 8),
        SW_CTX_USES_TYPE("the-implicit-return-type-is-a-use-of-the-type", "implicitlyReturned", 8),
        SW_CTX_USE("other-side-of-equality-gives-the-context", "compared", "fast", 4, "Mode.fast",
                   2, SW_MODE_CASES),
        SW_CTX_USE("assignment-target-gives-the-context", "assigned", "shared", 4, "Mode.shared",
                   4, SW_MODE_SHARED_DECL),
        SW_CTX_USE("switch-subject-gives-the-context", "switched", "fast", 5, "Mode.fast", 2,
                   SW_MODE_CASES),
        SW_CTX_USE("case-pattern-with-a-payload-names-the-case", "withPayload", "slow", 1,
                   "Mode.slow", 3, "    case slow(reason: String)"),
        SW_CTX_USE("parameter-gives-the-context", "argument", "shared", 5, "Mode.shared", 4,
                   SW_MODE_SHARED_DECL),
        SL_CALL_AT("implicit-member-call", SW_CTX_FILE, "Uses.called", ".make()", 0, SW_CTX_FILE,
                   "Mode.make", "Method", 5, "    static func make() -> Mode { .fast }"),
        SL_CALL_AT("enum-case-has-the-enum-type", SW_CTX_FILE, "Uses.called", "m.label()", 0,
                   SW_CTX_FILE, "Mode.label", "Method", 6, "    func label() {}"),
        SW_CTX_USE("case-with-a-payload-is-read", "payload", "slow", 2, "Mode.slow", 3,
                   "    case slow(reason: String)"),
        SW_CTX_TYPE_USE("the-annotation-beside-a-payload", "payload", 13),
        SW_CTX_TYPE_USE("the-annotation-beside-an-unknown-member", "noMember", 14),
        SW_CTX_USE("the-member-that-picked-the-overload", "pickByMember", "onlyAlpha", 1,
                   "Alpha.onlyAlpha", 8,
                   "class Alpha { static let onlyAlpha = Alpha(); static let both = Alpha() }"),
        SW_CTX_MADE("the-argument-made-for-that-type", "pickAlpha", "Alpha()", 3, "Ctx.Alpha", "Class",
                    8, "class Alpha { static let onlyAlpha = Alpha(); static let both = Alpha() }"),
        SW_CTX_MADE("the-argument-made-for-the-other-type", "pickBeta", "Beta()", 2, "Ctx.Beta",
                    "Class", 10, "class Beta { static let both = Beta() }"),
        SW_CTX_MADE("the-subclass-argument", "pickSubclass", "Omega()", 0, "Ctx.Omega", "Class", 9,
                    "class Omega: Alpha {}"),
        SW_CTX_MADE("the-struct-argument", "exactBeatsExistential", "Impl()", 0, "Ctx.Impl", "Struct",
                    12, "struct Impl: Proto { func required() {} }"),
        SW_CTX_CALL("member-only-one-parameter-type-has", "pickByMember", "pick(.onlyAlpha)",
                    SW_CTX_FILE, "Ctx.pick", 14, "func pick(_ a: Alpha) {}"),
        SW_CTX_CALL("argument-of-this-type", "pickAlpha", "pick(Alpha())", SW_CTX_FILE, "Ctx.pick",
                    14, "func pick(_ a: Alpha) {}"),
        SW_CTX_CALL("argument-of-that-type", "pickBeta", "pick(Beta())", SW_CTX2_FILE, "Ctx2.pick",
                    1, "func pick(_ b: Beta) {}"),
        SW_CTX_CALL("subclass-goes-to-its-base", "pickSubclass", "pick(Omega())", SW_CTX_FILE,
                    "Ctx.pick", 14, "func pick(_ a: Alpha) {}"),
        SW_CTX_CALL("exact-type-beats-an-existential", "exactBeatsExistential", "conv(Impl())",
                    SW_CTX2_FILE, "Ctx2.conv", 2, "func conv(_ i: Impl) {}"),
        SW_CTX_CALL("existential-goes-to-the-existential", "existentialTakesIt", "conv(p)",
                    SW_CTX_FILE, "Ctx.conv", 15, "func conv(_ p: any Proto) {}"),
        SW_CTX_CALL("literal-goes-to-the-type-that-takes-it", "literalTakesIt", "lit(\"x\")",
                    SW_CTX_FILE, "Ctx.lit", 16, "func lit(_ n: Named) {}"),
        SW_CTX_CALL("outside-type-cannot-be-a-project-class", "outsideGoesToTheProtocol", "out(u)",
                    SW_CTX2_FILE, "Ctx2.out", 4, "func out(_ p: any Proto) {}"),
        SW_CTX_CALL("trailing-closure-skips-a-default-to-reach-the-function", "trailing",
                    "closureLast { }", SW_CTX_FILE, "Ctx.closureLast", 20,
                    "func closureLast(queue: Beta = .both, done: () -> Void) {}"),
        SL_CALL_AT("function-beside-a-value-of-its-name", SW_CTX_FILE, "Uses.functionBesideValue",
                   "Point.mode(of: 1)", 0, SW_CTX_FILE, "Point.mode", "Method", 22,
                   "struct Point { let mode = 1; static func mode(of: Int) -> Point { Point() } }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("optional-none-is-not-the-enum-case", SW_CTX_FILE, "Uses.noOptionalNone",
                     "none", 1),
        SL_NO_USE_AT("no-context-no-member", SW_CTX_FILE, "Uses.noContext", "shared", 6),
        SL_NO_USE_AT("member-the-context-type-lacks", SW_CTX_FILE, "Uses.noMember", "other", 0),
        SL_NO_USE_AT("a-payload-binding-is-not-the-case-of-its-name", SW_CTX_FILE,
                     "Uses.withPayload", "fast", 7),
        SL_DENY_AT("both-parameter-types-have-the-member", SW_CTX_FILE, "Uses.noPickBoth",
                   "pick(.both)", 0),
        SL_DENY_AT("outside-type-fits-neither", SW_CTX_FILE, "Uses.noOutsideForEither", "pick(u)", 0),
    };
    ASSERT(sl_check("swift-context", ".swift", "func filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}


/* Types an initializer states, and `.foo` nested inside what a call of a type or a case takes. */
static const char SW_TYPED[] =
    "enum Reason {\n"                                                           /* 1 */
    "    case bad\n"                                                            /* 2 */
    "    case worse(detail: Detail)\n"                                          /* 3 */
    "    case either(Detail, other: Detail)\n"                                  /* 4 */
    "}\n"                                                                       /* 5 */
    "struct Detail {\n"                                                         /* 6 */
    "    let code: Int\n"                                                       /* 7 */
    "    static let standard = Detail(code: 1)\n"                               /* 8 */
    "    func show() {}\n"                                                      /* 9 */
    "}\n"                                                                       /* 10 */
    "enum Failure {\n"                                                          /* 11 */
    "    case failed(reason: Reason)\n"                                         /* 12 */
    "    case count(Int)\n"                                                     /* 13 */
    "    func describe() {}\n"                                                  /* 14 */
    "}\n"                                                                       /* 15 */
    "struct Place {\n"                                                          /* 16 */
    "    static let home = Place(name: 1)\n"                                    /* 17 */
    "    var name: Int\n"                                                       /* 18 */
    "    func walk() {}\n"                                                      /* 19 */
    "}\n"                                                                       /* 20 */
    "struct Memberwise { var place: Place; var other: Int }\n"                  /* 21 */
    "class Explicit { init(place: Place) {} }\n"                                /* 22 */
    "class Inherits: Explicit {}\n"                                             /* 23 */
    "class NoMemberwise { var place: Place = .home }\n"                         /* 24 */
    "class Box<T> {\n"                                                          /* 25 */
    "    init(place: Place) {}\n"                                               /* 26 */
    "    func go() {}\n"                                                        /* 27 */
    "}\n"                                                                       /* 28 */
    "class Plain {\n"                                                           /* 29 */
    "    func run() {}\n"                                                       /* 30 */
    "    static var standard: Plain { Plain() }\n"                              /* 31 */
    "    static let other = Plain()\n"                                          /* 32 */
    "    static func make() -> Plain { Plain() }\n"                             /* 33 */
    "    static func twice(a: Int) -> Plain { Plain() }\n"                      /* 34 */
    "    static func twice(a: String) -> Detail { .standard }\n"                /* 35 */
    "}\n"                                                                       /* 36 */
    "class Uses {\n"                                                            /* 37 */
    "    let read = Plain.standard\n"                                           /* 38 */
    "    let stored = Plain.other\n"                                            /* 39 */
    "    let made = Plain.make()\n"                                             /* 40 */
    "    let generic = Box<Int>(place: .home)\n"                                /* 41 */
    "    let chained = Uses.read2\n"                                            /* 42 */
    "    static let read2 = Plain.standard\n"                                   /* 43 */
    "    let noOverload = Plain.twice(a: 1)\n"                                  /* 44 */
    "    let noFunctionValue = Plain.make\n"                                    /* 45 */
    "    let noOutside = Other.standard\n"                                      /* 46 */
    "    func typedByRead() { read.run() }\n"                                   /* 47 */
    "    func typedByStored() { stored.run() }\n"                               /* 48 */
    "    func typedByCall() { made.run() }\n"                                   /* 49 */
    "    func typedByGeneric() { generic.go() }\n"                              /* 50 */
    "    func typedThroughTwo() { chained.run() }\n"                            /* 51 */
    "    func noOverloadType() { noOverload.run() }\n"                          /* 52 */
    "    func noFunctionValueType() { noFunctionValue.run() }\n"                /* 53 */
    "    func noOutsideType() { noOutside.run() }\n"                            /* 54 */
    "    func localGeneric() { let b = Box<Int>(place: .home); b.go() }\n"      /* 55 */
    "    func caseInCase() { let f: Failure = .failed(reason: .worse(detail: .standard)); use(f) }\n" /* 56 */
    "    func namedCase() { use(Failure.failed(reason: .bad)) }\n"              /* 57 */
    "    func unlabelledPayload() { use(Reason.either(.standard, other: .standard)) }\n" /* 58 */
    "    func caseHasItsEnum() { let f = Failure.failed(reason: .bad); f.describe() }\n" /* 59 */
    "    func noPayloadType() { use(Failure.count(.home)) }\n"                  /* 60 */
    "    func memberwise() { use(Memberwise(place: .home, other: 1)) }\n"       /* 61 */
    "    func initializer() { use(Explicit(place: .home)) }\n"                  /* 62 */
    "    func inherited() { use(Inherits(place: .home)) }\n"                    /* 63 */
    "    func noMemberwiseOfAClass() { use(NoMemberwise(place: .home)) }\n"     /* 64 */
    "    func noSuchLabel() { use(Memberwise(wrong: .home)) }\n"                /* 65 */
    "    func implicitInit() { let m: Memberwise = .init(place: .home, other: 1); use(m) }\n" /* 66 */
    "}\n"                                                                       /* 67 */
    "extension Outcome {\n"                                                     /* 68 */
    "    var success: Int? { nil }\n"                                           /* 69 */
    "    func instanceOnly() -> Outcome { self }\n"                             /* 70 */
    "    static func make() -> Outcome { fatalError() }\n"                      /* 71 */
    "}\n"                                                                       /* 72 */
    "func takesOutcome(_ o: Outcome) {}\n"                                      /* 73 */
    "class OutcomeUses {\n"                                                     /* 74 */
    "    func instanceRead(o: Outcome) { use(o.success) }\n"                    /* 75 */
    "    func noInstanceByImplicitMember() { takesOutcome(.success(1)) }\n"     /* 76 */
    "    func noInstanceByTypeName() { use(Outcome.success(1)) }\n"             /* 77 */
    "    func noInstanceFunctionByImplicitMember() { let r: Outcome = .instanceOnly(); use(r) }\n" /* 78 */
    "    func staticByImplicitMember() { let r: Outcome = .make(); use(r) }\n"  /* 79 */
    "    func noWitnessOfAnOutsideType(o: Outcome) { o.decode() }\n"            /* 80 */
    "}\n"                                                                       /* 81 */
    "protocol Decodes { func decode() }\n"                                      /* 82 */
    "extension Outcome: Decodes {}\n";                                          /* 83 */
/* Overloads across files that nothing tells apart, with one result type or with two. */
static const char SW_MANY[] = "class Many {\n"                                       /* 1 */
                              "    func pick(_ a: any First) -> Picked { Picked() }\n" /* 2 */
                              "    func split(_ a: any First) -> Picked { Picked() }\n" /* 3 */
                              "}\n"                                                  /* 4 */
                              "protocol First {}\n"                                  /* 5 */
                              "protocol Second {}\n"                                 /* 6 */
                              "struct Picked { func run() {} }\n"                    /* 7 */
                              "struct Other { func run() {} }\n"                     /* 8 */
                              "class ManyUses {\n"                                   /* 9 */
                              "    func commonResult(m: Many, v: Unknown) { m.pick(v).run() }\n" /* 10 */
                              "    func noCommonResult(m: Many, v: Unknown) { m.split(v).run() }\n" /* 11 */
                              "}\n";                                                 /* 12 */
static const char SW_MANY_EXT[] = "extension Many {\n"
                                  "    func pick(_ b: any Second) -> Picked { Picked() }\n"
                                  "    func split(_ b: any Second) -> Other { Other() }\n"
                                  "}\n";
/* One private name in two files of a module, and a file with no such type of its own. */
static const char SW_PRIVATE_A[] = "private enum Keys {\n"                       /* 1 */
                                   "    static let shared = 1\n"                 /* 2 */
                                   "    static func make() {}\n"                 /* 3 */
                                   "}\n"                                         /* 4 */
                                   "extension Keys {\n"                          /* 5 */
                                   "    static func added() {}\n"                /* 6 */
                                   "}\n"                                         /* 7 */
                                   "class UsesA {\n"                             /* 8 */
                                   "    func callsOwn() { Keys.make() }\n"       /* 9 */
                                   "    func readsOwn() { use(Keys.shared) }\n"  /* 10 */
                                   "    func callsExtension() { Keys.added() }\n" /* 11 */
                                   "}\n";                                        /* 12 */
static const char SW_PRIVATE_B[] = "fileprivate struct Keys {\n"                 /* 1 */
                                   "    static let shared = 2\n"                 /* 2 */
                                   "    static func make() {}\n"                 /* 3 */
                                   "}\n"                                         /* 4 */
                                   "class UsesB {\n"                             /* 5 */
                                   "    func callsOwn() { Keys.make() }\n"       /* 6 */
                                   "    func readsOwn() { use(Keys.shared) }\n"  /* 7 */
                                   "    func noOtherFilesExtension() { Keys.added() }\n" /* 8 */
                                   "}\n";                                        /* 9 */
static const char SW_PRIVATE_C[] = "class UsesC {\n"
                                   "    func noPrivateOfAnotherFile() { Keys.make() }\n"
                                   "}\n";
/* `#require` in a file that imports Swift Testing, and one in a file that does not. */
static const char SW_REQUIRE[] = "import Testing\n"
                                 "struct Item { func go() {}; static func make() -> Item? { nil } }\n"
                                 "class RequireUses {\n"
                                 "    func typedByRequire() throws { let i = try #require(Item.make()); i.go() }\n"
                                 "}\n";
static const char SW_NO_REQUIRE[] =
    "class NoRequireUses {\n"
    "    func noTestingImport() throws { let i = try #require(Item.make()); i.go() }\n"
    "}\n";
#define SW_TYPED_FILE "Sources/App/Typed.swift"
#define SW_PLAIN_RUN "    func run() {}"
#define SW_HOME_DECL "    static let home = Place(name: 1)"
#define SW_TYPED_CALL(id, method, needle, at, suffix, label, line, decl)                       \
    SL_CALL_AT(id, SW_TYPED_FILE, "Uses." method, needle, at, SW_TYPED_FILE, suffix, label, line, decl)
#define SW_TYPED_READ(id, method, needle, at, suffix, line, decl)                              \
    SL_USE_AT(id, SW_TYPED_FILE, "Uses." method, needle, at, SW_TYPED_FILE, suffix, "Variable", line, decl)
#define SW_TYPED_TYPE(id, method, needle, at, suffix, label, line, decl)                       \
    SL_USE_AT(id, SW_TYPED_FILE, "Uses." method, needle, at, SW_TYPED_FILE, suffix, label, line, decl)
#define SW_TYPED_RUN(id, method, needle)                                                       \
    SW_TYPED_CALL(id, method, needle, 0, "Plain.run", "Method", 30, SW_PLAIN_RUN)

TEST(resolver_source_swift_initializer_types_and_nested_context) {
    static const RQ_Source sources[] = {
        {.path = "Package.swift", .text = SW_MANIFEST},
        {.path = SW_TYPED_FILE, .text = SW_TYPED},
        {.path = "Sources/App/Many.swift", .text = SW_MANY},
        {.path = "Sources/App/ManyExt.swift", .text = SW_MANY_EXT},
        {.path = "Sources/App/PrivateA.swift", .text = SW_PRIVATE_A},
        {.path = "Sources/App/PrivateB.swift", .text = SW_PRIVATE_B},
        {.path = "Sources/App/PrivateC.swift", .text = SW_PRIVATE_C},
        {.path = "Sources/App/Require.swift", .text = SW_REQUIRE},
        {.path = "Sources/App/NoRequire.swift", .text = SW_NO_REQUIRE},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("overloads-that-all-return-one-type-say-what-follows", "Sources/App/Many.swift",
                   "ManyUses.commonResult", "m.pick(v).run()", 0, "Sources/App/Many.swift", "Picked.run",
                   "Method", 7, "struct Picked { func run() {} }"),
        SL_CALL_AT("a-private-type-is-its-files-own-call", "Sources/App/PrivateA.swift", "UsesA.callsOwn",
                   "Keys.make()", 0, "Sources/App/PrivateA.swift", "PrivateA.Keys.make", "Method", 3,
                   "    static func make() {}"),
        SL_USE_AT("a-private-type-is-its-files-own-read", "Sources/App/PrivateA.swift", "UsesA.readsOwn",
                  "shared", 1, "Sources/App/PrivateA.swift", "PrivateA.Keys.shared", "Variable", 2,
                  "    static let shared = 1"),
        SL_USE_AT("the-private-type-read-for-that-member", "Sources/App/PrivateA.swift",
                  "UsesA.readsOwn", "Keys", 3, "Sources/App/PrivateA.swift", "PrivateA.Keys", "Class", 5,
                  "extension Keys {"),
        SL_CALL_AT("an-extension-in-its-file-is-that-type", "Sources/App/PrivateA.swift",
                   "UsesA.callsExtension", "Keys.added()", 0, "Sources/App/PrivateA.swift",
                   "PrivateA.Keys.added", "Method", 6, "    static func added() {}"),
        SL_CALL_AT("the-same-name-in-another-file-call", "Sources/App/PrivateB.swift", "UsesB.callsOwn",
                   "Keys.make()", 0, "Sources/App/PrivateB.swift", "PrivateB.Keys.make", "Method", 3,
                   "    static func make() {}"),
        SL_USE_AT("the-same-name-in-another-file-read", "Sources/App/PrivateB.swift", "UsesB.readsOwn",
                  "shared", 1, "Sources/App/PrivateB.swift", "PrivateB.Keys.shared", "Variable", 2,
                  "    static let shared = 2"),
        SL_USE_AT("the-type-read-for-that-member", "Sources/App/PrivateB.swift", "UsesB.readsOwn", "Keys",
                  2, "Sources/App/PrivateB.swift", "PrivateB.Keys", "Struct", 1,
                  "fileprivate struct Keys {"),
        SL_CALL_AT("require-is-the-value-it-unwraps", "Sources/App/Require.swift",
                   "RequireUses.typedByRequire", "i.go()", 0, "Sources/App/Require.swift", "Item.go",
                   "Method", 2, "struct Item { func go() {}; static func make() -> Item? { nil } }"),
        SL_CALL_AT("that-call-without-the-testing-import", "Sources/App/NoRequire.swift",
                   "NoRequireUses.noTestingImport", "Item.make()", 0, "Sources/App/Require.swift",
                   "Item.make", "Method", 2,
                   "struct Item { func go() {}; static func make() -> Item? { nil } }"),
        SL_CALL_AT("the-call-require-holds", "Sources/App/Require.swift", "RequireUses.typedByRequire",
                   "Item.make()", 0, "Sources/App/Require.swift", "Item.make", "Method", 2,
                   "struct Item { func go() {}; static func make() -> Item? { nil } }"),
        SW_TYPED_RUN("property-is-what-the-member-it-reads-is", "typedByRead", "read.run()"),
        SW_TYPED_RUN("property-from-a-stored-static", "typedByStored", "stored.run()"),
        SW_TYPED_RUN("property-is-what-the-function-it-calls-returns", "typedByCall", "made.run()"),
        SW_TYPED_CALL("property-from-a-generic-call-of-a-type", "typedByGeneric", "generic.go()", 0,
                      "Box.go", "Method", 27, "    func go() {}"),
        SW_TYPED_RUN("property-through-another-property", "typedThroughTwo", "chained.run()"),
        SW_TYPED_CALL("local-from-a-generic-call-of-a-type", "localGeneric", "b.go()", 0, "Box.go",
                      "Method", 27, "    func go() {}"),
        SW_TYPED_CALL("generic-call-of-a-type-names-it", "localGeneric", "Box<Int>(place: .home)", 1,
                      "Typed.Box", "Class", 25, "class Box<T> {"),
        SW_TYPED_TYPE("the-generic-type-it-spells", "localGeneric", "Box", 2, "Typed.Box", "Class", 25,
                      "class Box<T> {"),
        SW_TYPED_READ("argument-of-a-generic-call-of-a-type", "localGeneric", "home", 3,
                      "Place.home", 17, SW_HOME_DECL),
        SW_TYPED_TYPE("the-annotation-of-a-case-value", "caseInCase", "Failure", 1, "Typed.Failure",
                      "Class", 11, "enum Failure {"),
        SW_TYPED_READ("a-case-called-with-its-payload-is-read", "caseInCase", "failed", 1,
                      "Failure.failed", 12, "    case failed(reason: Reason)"),
        SW_TYPED_READ("case-inside-a-case", "caseInCase", "worse", 1, "Reason.worse", 3,
                      "    case worse(detail: Detail)"),
        SW_TYPED_READ("static-inside-a-case-inside-a-case", "caseInCase", "standard", 6,
                      "Detail.standard", 8, "    static let standard = Detail(code: 1)"),
        SW_TYPED_TYPE("the-head-of-a-named-case", "namedCase", "Failure", 2, "Typed.Failure", "Class",
                      11, "enum Failure {"),
        SW_TYPED_READ("a-named-case-is-read-when-called", "namedCase", "failed", 2, "Failure.failed",
                      12, "    case failed(reason: Reason)"),
        SW_TYPED_READ("case-a-named-case-takes", "namedCase", "bad", 1, "Reason.bad", 2,
                      "    case bad"),
        SW_TYPED_TYPE("the-head-of-a-case-with-two-payloads", "unlabelledPayload", "Reason", 2,
                      "Typed.Reason", "Class", 1, "enum Reason {"),
        SW_TYPED_READ("a-case-with-two-payloads", "unlabelledPayload", "either", 1, "Reason.either", 4,
                      "    case either(Detail, other: Detail)"),
        SW_TYPED_READ("the-unlabelled-payload", "unlabelledPayload", "standard", 7,
                      "Detail.standard", 8, "    static let standard = Detail(code: 1)"),
        SW_TYPED_READ("the-labelled-payload-beside-it", "unlabelledPayload", "standard", 8,
                      "Detail.standard", 8, "    static let standard = Detail(code: 1)"),
        SW_TYPED_CALL("a-case-with-its-payload-is-of-its-enum", "caseHasItsEnum", "f.describe()", 0,
                      "Failure.describe", "Method", 14, "    func describe() {}"),
        SW_TYPED_READ("stored-property-of-a-struct-with-no-initializer", "memberwise", "home", 5,
                      "Place.home", 17, SW_HOME_DECL),
        SW_TYPED_READ("parameter-of-the-initializer", "initializer", "home", 6, "Place.home", 17,
                      SW_HOME_DECL),
        SW_TYPED_READ("parameter-of-an-inherited-initializer", "inherited", "home", 7, "Place.home",
                      17, SW_HOME_DECL),
        SW_TYPED_TYPE("the-annotation-of-an-implicit-init", "implicitInit", "Memberwise", 6,
                      "Typed.Memberwise", "Struct", 21,
                      "struct Memberwise { var place: Place; var other: Int }"),
        SW_TYPED_READ("argument-of-an-implicit-init", "implicitInit", "home", 10, "Place.home", 17,
                      SW_HOME_DECL),
        SW_TYPED_TYPE("the-head-of-a-case-of-an-outside-payload", "noPayloadType", "Failure", 4,
                      "Typed.Failure", "Class", 11, "enum Failure {"),
        SW_TYPED_READ("the-case-of-an-outside-payload", "noPayloadType", "count", 1, "Failure.count",
                      13, "    case count(Int)"),
        SL_USE_AT("an-instance-reads-the-instance-property", SW_TYPED_FILE, "OutcomeUses.instanceRead",
                  "success", 1, SW_TYPED_FILE, "Outcome.success", "Variable", 69,
                  "    var success: Int? { nil }"),
        SL_CALL_AT("a-static-function-by-implicit-member", SW_TYPED_FILE,
                   "OutcomeUses.staticByImplicitMember", ".make()", 1, SW_TYPED_FILE, "Outcome.make",
                   "Method", 71, "    static func make() -> Outcome { fatalError() }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("overloads-that-return-different-types", SW_TYPED_FILE, "Uses.noOverloadType",
                   "noOverload.run()", 0),
        SL_DENY_AT("overloads-across-files-with-two-result-types", "Sources/App/Many.swift",
                   "ManyUses.noCommonResult", "m.split(v).run()", 0),
        SL_DENY_AT("an-extension-of-the-other-files-private-type", "Sources/App/PrivateB.swift",
                   "UsesB.noOtherFilesExtension", "Keys.added()", 0),
        SL_DENY_AT("a-private-type-of-another-file", "Sources/App/PrivateC.swift",
                   "UsesC.noPrivateOfAnotherFile", "Keys.make()", 0),
        SL_DENY_AT("no-testing-import-no-require", "Sources/App/NoRequire.swift",
                   "NoRequireUses.noTestingImport", "i.go()", 0),
        SL_DENY_AT("a-function-not-called-is-not-its-result", SW_TYPED_FILE,
                   "Uses.noFunctionValueType", "noFunctionValue.run()", 0),
        SL_DENY_AT("a-type-outside-the-project", SW_TYPED_FILE, "Uses.noOutsideType",
                   "noOutside.run()", 0),
        SL_NO_USE_AT("a-payload-of-an-outside-type", SW_TYPED_FILE, "Uses.noPayloadType", "home", 4),
        SL_NO_USE_AT("a-class-has-no-memberwise-initializer", SW_TYPED_FILE,
                     "Uses.noMemberwiseOfAClass", "home", 8),
        SL_NO_USE_AT("a-label-no-stored-property-has", SW_TYPED_FILE, "Uses.noSuchLabel", "home", 9),
        SL_NO_USE_AT("implicit-member-is-never-an-instance-property", SW_TYPED_FILE,
                     "OutcomeUses.noInstanceByImplicitMember", "success", 2),
        SL_NO_USE_AT("a-type-name-does-not-reach-an-instance-property", SW_TYPED_FILE,
                     "OutcomeUses.noInstanceByTypeName", "success", 3),
        SL_DENY_AT("an-outside-type-meets-a-protocol-with-its-own-member", SW_TYPED_FILE,
                   "OutcomeUses.noWitnessOfAnOutsideType", "o.decode()", 0),
        SL_DENY_AT("implicit-member-is-never-an-instance-function", SW_TYPED_FILE,
                   "OutcomeUses.noInstanceFunctionByImplicitMember", ".instanceOnly()", 0),
    };
    ASSERT(sl_check("swift-typed", ".swift", "func filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* A function type with no parameters still has a signature: the closure given to it has no $0,
 * and its body resolves as any other. The signature is read long after the declaration was walked,
 * so it has to outlive that walk: run with detect_stack_use_after_return=1 this was a dead frame. */
static const char SW_NOARGS[] =
    "struct Dog { func bark() {} }\n"                  /* 1 */
    "func noArgs(_ body: () -> Void) {}\n"             /* 2 */
    "func maybe(_ body: (() -> Void)?) {}\n"           /* 3 */
    "class Flow {\n"                                   /* 4 */
    "    func plain() { noArgs { Dog().bark() } }\n"   /* 5 */
    "    func optional() { maybe { Dog().bark() } }\n" /* 6 */
    "    func noFirst() { noArgs { $0.bark() } }\n"    /* 7 */
    "}\n";                                             /* 8 */
#define SW_NOARGS_FILE "Sources/App/NoArgs.swift"
#define SW_NOARGS_CALL(id, method, needle, at, suffix, label, line, decl)                          \
    SL_CALL_AT(id, SW_NOARGS_FILE, "Flow." method, needle, at, SW_NOARGS_FILE, suffix, label, line, decl)

TEST(resolver_source_swift_a_closure_of_no_parameters) {
    static const RQ_Source sources[] = {
        {.path = "Package.swift", .text = SW_MANIFEST},
        {.path = SW_NOARGS_FILE, .text = SW_NOARGS},
    };
    static const RQ_Expected expected[] = {
        SW_NOARGS_CALL("the-function-given-it", "plain", "noArgs { Dog().bark() }", 0, "NoArgs.noArgs",
                       "Function", 2, "func noArgs(_ body: () -> Void) {}"),
        SW_NOARGS_CALL("a-call-in-its-body", "plain", "Dog().bark()", 0, "Dog.bark", "Method", 1,
                       "struct Dog { func bark() {} }"),
        SW_NOARGS_CALL("the-function-given-an-optional-one", "optional", "maybe { Dog().bark() }", 0,
                       "NoArgs.maybe", "Function", 3, "func maybe(_ body: (() -> Void)?) {}"),
        SW_NOARGS_CALL("a-call-in-that-body", "optional", "Dog().bark()", 0, "Dog.bark", "Method", 1,
                       "struct Dog { func bark() {} }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("no-parameters-no-first-shorthand", SW_NOARGS_FILE, "Flow.noFirst", "$0.bark()", 0),
    };
    ASSERT(sl_check("swift-noargs", ".swift", "func filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* Types a closure's signature and a value's form state, and what they leave open. */
static const char SW_FLOW[] =
    "struct Dog { func bark() {} }\n"                                           /* 1 */
    "struct Cat { func bark() {} }\n"                                           /* 2 */
    "protocol Pet {}\n"                                                         /* 3 */
    "enum Oops: Error { case bad; func report() {} }\n"                         /* 4 */
    "func withDog(_ body: (Dog) -> Void) {}\n"                                  /* 5 */
    "func withBoth(_ body: (Dog, Cat) -> Void) {}\n"                            /* 6 */
    "func labelled(done: @escaping (Dog) -> Void) {}\n"                         /* 7 */
    "func optionalDog() -> Dog? { nil }\n"                                      /* 8 */
    "func anyValue() -> Any { 1 }\n"                                            /* 9 */
    "class Flow {\n"                                                            /* 10 */
    "    func named() { withDog { d in d.bark() } }\n"                          /* 11 */
    "    func shorthand() { withDog { $0.bark() } }\n"                          /* 12 */
    "    func second() { withBoth { _, c in c.bark() } }\n"                     /* 13 */
    "    func secondShorthand() { withBoth { $1.bark() } }\n"                   /* 14 */
    "    func byLabel() { labelled(done: { d in d.bark() }) }\n"                /* 15 */
    "    func written() { unknown { (d: Dog) in d.bark() } }\n"                 /* 16 */
    "    func noSignature() { unknown { d in d.bark() } }\n"                    /* 17 */
    "    func noShorthand() { unknown { $0.bark() } }\n"                        /* 18 */
    "    func noOuterShorthand() { withDog { _ in unknown { $0.bark() } } }\n"  /* 19 */
    "    func cast() { if let d = anyValue() as? Dog { d.bark() } }\n"          /* 20 */
    "    func forced() { (anyValue() as! Dog).bark() }\n"                       /* 21 */
    "    func either(c: Bool) { let d = c ? Dog() : Dog(); d.bark() }\n"        /* 22 */
    "    func noEither(c: Bool) { let d = c ? Dog() : Cat(); d.bark() }\n"      /* 23 */
    "    func coalesced() { let d = optionalDog() ?? Dog(); d.bark() }\n"       /* 24 */
    "    func caught() { do { try thrower() } catch let e as Oops { e.report() } catch { } }\n" /* 25 */
    "    func noCaught() { do { try thrower() } catch { error.report() } }\n"   /* 26 */
    "    func caseAs(v: Any) { switch v { case let d as Dog: d.bark(); default: break } }\n" /* 27 */
    "    func thrower() throws {}\n"                                            /* 28 */
    "}\n";                                                                      /* 29 */
#define SW_FLOW_FILE "Sources/App/Flow.swift"
#define SW_FLOW_CALL(id, method, needle, at, suffix, label, line, decl)                        \
    SL_CALL_AT(id, SW_FLOW_FILE, "Flow." method, needle, at, SW_FLOW_FILE, suffix, label, line, decl)
#define SW_DOG_BARK "struct Dog { func bark() {} }"
#define SW_FLOW_DOG(id, method, needle, at) SW_FLOW_CALL(id, method, needle, at, "Dog.bark", "Method", 1, SW_DOG_BARK)
#define SW_FLOW_NO(id, method, needle, at) SL_DENY_AT(id, SW_FLOW_FILE, "Flow." method, needle, at)

TEST(resolver_source_swift_closures_and_value_forms) {
    static const RQ_Source sources[] = {
        {.path = "Package.swift", .text = SW_MANIFEST},
        {.path = SW_FLOW_FILE, .text = SW_FLOW},
    };
    static const RQ_Expected expected[] = {
        SW_FLOW_CALL("the-function-given-a-closure", "named", "withDog { d in d.bark() }", 0, "Flow.withDog",
                     "Function", 5, "func withDog(_ body: (Dog) -> Void) {}"),
        SW_FLOW_DOG("signature-types-a-named-closure-parameter", "named", "d.bark()", 0),
        SW_FLOW_CALL("the-function-given-a-shorthand-closure", "shorthand", "withDog { $0.bark() }", 0,
                     "Flow.withDog", "Function", 5, "func withDog(_ body: (Dog) -> Void) {}"),
        SW_FLOW_DOG("signature-types-a-shorthand", "shorthand", "$0.bark()", 0),
        SW_FLOW_CALL("the-function-of-two-parameters", "second", "withBoth { _, c in c.bark() }", 0,
                     "Flow.withBoth", "Function", 6, "func withBoth(_ body: (Dog, Cat) -> Void) {}"),
        SW_FLOW_CALL("signature-types-by-position", "second", "c.bark()", 0, "Cat.bark", "Method", 2,
                     "struct Cat { func bark() {} }"),
        SW_FLOW_CALL("that-function-with-a-shorthand", "secondShorthand", "withBoth { $1.bark() }", 0,
                     "Flow.withBoth", "Function", 6, "func withBoth(_ body: (Dog, Cat) -> Void) {}"),
        SW_FLOW_CALL("signature-types-the-second-shorthand", "secondShorthand", "$1.bark()", 0, "Cat.bark",
                     "Method", 2, "struct Cat { func bark() {} }"),
        SW_FLOW_CALL("the-function-given-a-labelled-closure", "byLabel", "labelled(done: { d in d.bark() })", 0,
                     "Flow.labelled", "Function", 7, "func labelled(done: @escaping (Dog) -> Void) {}"),
        SW_FLOW_DOG("signature-types-a-labelled-closure", "byLabel", "d.bark()", 1),
        SW_FLOW_DOG("a-written-closure-parameter-type", "written", "d.bark()", 2),
        SW_FLOW_CALL("the-function-beside-a-conditional-cast", "cast", "anyValue()", 1, "Flow.anyValue",
                     "Function", 9, "func anyValue() -> Any { 1 }"),
        SW_FLOW_DOG("a-conditional-cast-names-its-type", "cast", "d.bark()", 4),
        SW_FLOW_CALL("the-function-beside-a-forced-cast", "forced", "anyValue()", 2, "Flow.anyValue",
                     "Function", 9, "func anyValue() -> Any { 1 }"),
        SW_FLOW_DOG("a-forced-cast-in-parentheses", "forced", "(anyValue() as! Dog).bark()", 0),
        SW_FLOW_CALL("one-arm-of-a-ternary", "either", "Dog()", 1, "Flow.Dog", "Struct", 1, SW_DOG_BARK),
        SW_FLOW_CALL("the-other-arm-of-a-ternary", "either", "Dog()", 2, "Flow.Dog", "Struct", 1, SW_DOG_BARK),
        SW_FLOW_CALL("one-arm-of-two-types", "noEither", "Dog()", 3, "Flow.Dog", "Struct", 1, SW_DOG_BARK),
        SW_FLOW_CALL("the-other-arm-of-two-types", "noEither", "Cat()", 0, "Flow.Cat", "Struct", 2,
                     "struct Cat { func bark() {} }"),
        SW_FLOW_CALL("the-function-given-a-closure-holding-another", "noOuterShorthand",
                     "withDog { _ in unknown { $0.bark() } }", 0, "Flow.withDog", "Function", 5,
                     "func withDog(_ body: (Dog) -> Void) {}"),
        SW_FLOW_DOG("both-arms-of-one-type", "either", "d.bark()", 5),
        SW_FLOW_CALL("the-function-before-a-coalescing", "coalesced", "optionalDog()", 1, "Flow.optionalDog",
                     "Function", 8, "func optionalDog() -> Dog? { nil }"),
        SW_FLOW_CALL("the-value-after-a-coalescing", "coalesced", "Dog()", 5, "Flow.Dog", "Struct", 1, SW_DOG_BARK),
        SW_FLOW_DOG("coalescing-two-of-one-type", "coalesced", "d.bark()", 7),
        SW_FLOW_CALL("the-call-a-catch-follows", "caught", "thrower()", 0, "Flow.thrower", "Method", 28,
                     "    func thrower() throws {}"),
        SW_FLOW_CALL("a-catch-with-a-type-names-it", "caught", "e.report()", 0, "Oops.report", "Method", 4,
                     "enum Oops: Error { case bad; func report() {} }"),
        SW_FLOW_CALL("the-call-a-plain-catch-follows", "noCaught", "thrower()", 1, "Flow.thrower", "Method", 28,
                     "    func thrower() throws {}"),
        SW_FLOW_DOG("a-case-with-a-type-names-it", "caseAs", "d.bark()", 8),
    };
    static const RQ_Forbidden forbidden[] = {
        SW_FLOW_NO("no-signature-no-type", "noSignature", "d.bark()", 3),
        SW_FLOW_NO("no-signature-no-shorthand-type", "noShorthand", "$0.bark()", 1),
        SW_FLOW_NO("a-shorthand-is-its-own-closures", "noOuterShorthand", "$0.bark()", 2),
        SW_FLOW_NO("arms-of-two-types", "noEither", "d.bark()", 6),
        SW_FLOW_NO("a-plain-catch-has-no-type", "noCaught", "error.report()", 0),
    };
    ASSERT(sl_check("swift-flow", ".swift", "func filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* Forms from bloc and dio. */
static const char DART2_MAIN[] =
    "import 'package:shared/shared.dart';\n"                                    /* 1 */
    "import 'base.dart';\n"                                                     /* 2 */
    "export 'base.dart' hide Hidden;\n"                                         /* 3 */
    "part 'main_part.dart';\n"                                                  /* 4 */
    "enum Status { ready, busy }\n"                                             /* 5 */
    "void busy() {}\n"                                                          /* 6 */
    "const topValue = 1;\n"                                                     /* 7 */
    "class Caller extends Base<Status> with Helper implements Marker {\n"       /* 8 */
    "  Caller(int seed) : super(Status.busy) { ctorBody(); }\n"                 /* 9 */
    "  void ctorBody() {}\n"                                                    /* 10 */
    "  void onBase() {}\n"                                                      /* 11 */
    "  void readsEnum() { use(Status.ready); }\n"                               /* 12 */
    "  void readsCase(Status s) {\n"                                            /* 13 */
    "    switch (s) {\n"                                                        /* 14 */
    "      case Status.busy:\n"                                                 /* 15 */
    "        break;\n"                                                          /* 16 */
    "      default:\n"                                                          /* 17 */
    "    }\n"                                                                   /* 18 */
    "  }\n"                                                                     /* 19 */
    "  void superCall() { super.onBase(); }\n"                                  /* 20 */
    "  void callsValue(Emitter<int> emit) { emit(1); }\n"                       /* 21 */
    "  void noValueCall(Plain plain) { plain(1); }\n"                           /* 22 */
    "  void typedPattern(Object o) {\n"                                         /* 23 */
    "    switch (o) {\n"                                                        /* 24 */
    "      case Sub s:\n"                                                       /* 25 */
    "        s.subGo();\n"                                                      /* 26 */
    "      default:\n"                                                          /* 27 */
    "    }\n"                                                                   /* 28 */
    "  }\n"                                                                     /* 29 */
    "  void abstractMember(Emitter<int> e) { e.finish(); }\n"                   /* 30 */
    "  void mixinMember() { fromHelper(); }\n"                                  /* 31 */
    "  void mixinPrivate() { _hidden(); }\n"                                    /* 32 */
    "  void getterRead(Sub s) { use(s.flag); }\n"                               /* 33 */
    "  void topRead() { use(topValue); }\n"                                     /* 34 */
    "  void pathDep() { sharedFn(); }\n"                                        /* 35 */
    "  void noBareConstant() { use(ready); }\n"                                 /* 36 */
    "  void noUnknownReceiver(x) { use(x.ready); }\n"                           /* 37 */
    "  void noCalledMember(Sub s) { s.subGo(); }\n"                             /* 38 */
    "}\n"                                                                       /* 39 */
    "class Bound<Store extends Sub> { void noTypeParam(Store s) { s.subGo(); } }\n" /* 40 */
    "class Sub { bool get flag => true; void subGo() {} }\n"                    /* 41 */
    "class Store { void subGo() {} }\n"                                         /* 42 */
    "class Plain { void other() {} }\n"                                         /* 43 */
    "class Defaults {\n"                                                        /* 44 */
    "  const Defaults({this.kept = Status.ready, Sub? nothing});\n"             /* 45 */
    "  final Status kept;\n"                                                    /* 46 */
    "  void named({Status s = Status.busy}) {}\n"                               /* 47 */
    "  void noParameterRead({Status shadowed = Status.ready}) { use(shadowed); }\n" /* 48 */
    "  bool get shadowed => true;\n"                                            /* 49 */
    "}\n"                                                                       /* 50 */
    "void positional([Status s = Status.busy]) {}\n"                            /* 51 */
    "class Pair {\n"                                                            /* 52 */
    "  int get both => 1;\n"                                                    /* 53 */
    "  set both(int v) {}\n"                                                    /* 54 */
    "  void readsBoth() { use(both); }\n"                                       /* 55 */
    "}\n";                                                                      /* 56 */
static const char DART2_HEADER[] = "import 'main.dart';\n"
                                   "import 'base.dart';\n"
                                   "class Head extends Base<Status> with Helper implements Marker {}\n";
static const char DART2_TOP[] = "import 'main.dart';\n"
                                "final Status typed = Status.ready;\n";
static const char DART2_CTOR[] = "import 'main.dart';\n"
                                 "class WithDefault {\n"
                                 "  const WithDefault({this.kept = Status.ready});\n"
                                 "  final Status kept;\n"
                                 "}\n";
static const char DART2_BOX[] =
    "class Box<T> {\n"                                              /* 1 */
    "  Box(this.held);\n"                                           /* 2 */
    "  final T held;\n"                                             /* 3 */
    "  T get value => held;\n"                                      /* 4 */
    "  T make() => held;\n"                                         /* 5 */
    "  void noOwnParameter() { value.itemGo(); }\n"                 /* 6 */
    "}\n"                                                           /* 7 */
    "class Item { void itemGo() {} }\n"                             /* 8 */
    "class T { void itemGo() {} }\n"                                /* 9 */
    "class ItemBox extends Box<Item> {\n"                           /* 10 */
    "  ItemBox() : super(Item());\n"                                /* 11 */
    "  void viaGetter() { value.itemGo(); }\n"                      /* 12 */
    "  void viaMethod() { make().itemGo(); }\n"                     /* 13 */
    "  void viaField() { this.held.itemGo(); }\n"                   /* 14 */
    "}\n"                                                           /* 15 */
    "class Mid<U> extends Box<U> { Mid(U u) : super(u); }\n"        /* 16 */
    "class Deep extends Mid<Item> {\n"                              /* 17 */
    "  Deep() : super(Item());\n"                                   /* 18 */
    "  void viaTwoLevels() { value.itemGo(); }\n"                   /* 19 */
    "}\n"                                                           /* 20 */
    "class Open<V> extends Box<V> {\n"                              /* 21 */
    "  Open(V v) : super(v);\n"                                     /* 22 */
    "  void noParameterLeftOpen() { value.itemGo(); }\n"            /* 23 */
    "}\n"                                                           /* 24 */
    "class Listed extends Box<List<Item>> {\n"                      /* 25 */
    "  Listed() : super([]);\n"                                     /* 26 */
    "  void noElementType() { value.itemGo(); }\n"                  /* 27 */
    "}\n";                                                          /* 28 */
static const char DART2_BASE[] = "abstract class Base<T> {\n"                   /* 1 */
                                 "  Base(T value);\n"                           /* 2 */
                                 "  void onBase() {}\n"                         /* 3 */
                                 "}\n"                                          /* 4 */
                                 "abstract class Emitter<S> {\n"                /* 5 */
                                 "  void call(S state);\n"                      /* 6 */
                                 "  void finish();\n"                           /* 7 */
                                 "}\n"                                          /* 8 */
                                 "mixin Helper on Base<int> {\n"                /* 9 */
                                 "  void fromHelper() { _hidden(); }\n"         /* 10 */
                                 "  void _hidden() {}\n"                        /* 11 */
                                 "}\n"                                          /* 12 */
                                 "class Marker {}\n"                            /* 13 */
                                 "class Hidden {}\n";                           /* 14 */

#define DART2_FILE "lib/main.dart"
#define DART2_BASE_FILE "lib/base.dart"
#define DART2_CALL(id, method, text, at, tfile, tsuffix, label, tline, tdecl)                  \
    SL_CALL_AT(id, DART2_FILE, "Caller." method, text, at, tfile, tsuffix, label, tline, tdecl)
#define DART2_USE(id, method, text, at, tsuffix, label, tline, tdecl)                          \
    SL_USE_AT(id, DART2_FILE, "Caller." method, text, at, DART2_FILE, tsuffix, label, tline, tdecl)
#define DART2_IMPORT(id, sfile, scaller, text, at, tfile, tsuffix, tdecl)                      \
    {                                                                                          \
        .case_id = id,                                                                         \
        .site = {.file = sfile, .caller = scaller, .needle = text, .occurrence_index = at},    \
        .relation = "IMPORTS",                                                                 \
        .target = {.file = tfile,                                                              \
                   .suffix = tsuffix,                                                          \
                   .label = "File",                                                            \
                   .declaration_line = 1,                                                      \
                   .declaration_text = tdecl}                                                  \
    }

TEST(resolver_source_dart_headers_reads_members_and_pubspec_paths) {
    static const RQ_Source sources[] = {
        {.path = "pubspec.yaml",
         .text = "name: app\ndependencies:\n  shared:\n    path: packages/one\n"},
        {.path = DART2_FILE, .text = DART2_MAIN},
        {.path = "lib/main_part.dart", .text = "part of 'main.dart';\nvoid fromPart() {}\n"},
        {.path = DART2_BASE_FILE, .text = DART2_BASE},
        {.path = "lib/box.dart", .text = DART2_BOX},
        {.path = "lib/top.dart", .text = DART2_TOP},
        {.path = "lib/header.dart", .text = DART2_HEADER},
        {.path = "lib/ctor.dart", .text = DART2_CTOR},
        {.path = "packages/one/pubspec.yaml", .text = "name: shared\n"},
        {.path = "packages/one/lib/shared.dart",
         .text = "import 'package:shared/extra.dart';\n"
                 "void sharedFn() { extraFn(); }\n"},
        {.path = "packages/one/lib/extra.dart", .text = "void extraFn() {}\n"},
        {.path = "packages/two/pubspec.yaml", .text = "name: shared\n"},
        {.path = "packages/two/lib/shared.dart",
         .text = "import 'package:shared/extra.dart';\n"
                 "void sharedFn() { extraFn(); }\n"},
        {.path = "packages/two/lib/extra.dart", .text = "void extraFn() {}\n"},
        {.path = "packages/three/pubspec.yaml",
         .text = "name: three\ndependencies:\n  shared:\n    path: ../two\n"},
        {.path = "packages/three/lib/three.dart",
         .text = "import 'package:shared/shared.dart';\n"
                 "void callsTwo() { sharedFn(); }\n"},
        {.path = "packages/four/pubspec.yaml",
         .text = "name: four\ndependencies:\n  shared: ^1.0.0\n"},
        {.path = "packages/four/lib/four.dart",
         .text = "import 'package:shared/shared.dart';\n"
                 "void noPathDep() { sharedFn(); }\n"},
    };
    static const RQ_Expected expected[] = {
        DART2_IMPORT("import-names-a-file", DART2_FILE, "main.dart.__file__", "'base.dart'", 0,
                     DART2_BASE_FILE, "base.dart.__file__", "abstract class Base<T> {"),
        DART2_IMPORT("export-names-a-file", DART2_FILE, "main.dart.__file__", "'base.dart'", 1,
                     DART2_BASE_FILE, "base.dart.__file__", "abstract class Base<T> {"),
        DART2_IMPORT("part-names-a-file", DART2_FILE, "main.dart.__file__", "'main_part.dart'", 0,
                     "lib/main_part.dart", "main_part.dart.__file__", "part of 'main.dart';"),
        DART2_IMPORT("path-dependency-decides-a-shared-name", DART2_FILE, "main.dart.__file__",
                     "'package:shared/shared.dart'", 0, "packages/one/lib/shared.dart",
                     "lib.shared.dart.__file__", "import 'package:shared/extra.dart';"),
        DART2_USE("enum-constant-read", "readsEnum", "ready", 1, "Status.ready", "Variable", 5,
                  "enum Status { ready, busy }"),
        DART2_USE("enum-head-read", "readsEnum", "Status", 3, "main.Status", "Enum", 5,
                  "enum Status { ready, busy }"),
        DART2_USE("enum-constant-in-a-case", "readsCase", "busy", 3, "Status.busy", "Variable", 5,
                  "enum Status { ready, busy }"),
        DART2_USE("enum-head-in-a-case", "readsCase", "Status", 5, "main.Status", "Enum", 5,
                  "enum Status { ready, busy }"),
        DART2_CALL("super-selects-past-the-own-member", "superCall", "super.onBase()", 0,
                   DART2_BASE_FILE, "Base.onBase", "Method", 3, "  void onBase() {}"),
        DART2_CALL("a-value-invoked-runs-its-call-method", "callsValue", "emit(1)", 0,
                   DART2_BASE_FILE, "Emitter.call", "Method", 6, "  void call(S state);"),
        DART2_CALL("typed-pattern-binds-its-name", "typedPattern", "s.subGo", 0, DART2_FILE,
                   "Sub.subGo", "Method", 41, "class Sub { bool get flag => true; void subGo() {} }"),
        DART2_CALL("abstract-member-has-a-node", "abstractMember", "e.finish", 0, DART2_BASE_FILE,
                   "Emitter.finish", "Method", 7, "  void finish();"),
        DART2_CALL("mixin-member-has-a-node", "mixinMember", "fromHelper()", 0, DART2_BASE_FILE,
                   "Helper.fromHelper", "Method", 10, "  void fromHelper() { _hidden(); }"),
        DART2_USE("getter-read", "getterRead", "flag", 0, "Sub.flag", "Method", 41,
                  "class Sub { bool get flag => true; void subGo() {} }"),
        SL_USE_AT("a-getter-beside-its-setter", DART2_FILE, "Pair.readsBoth", "both", 2, DART2_FILE,
                  "Pair.both", "Method", 54, "  set both(int v) {}"),
        SL_USE_AT("default-of-a-named-parameter", DART2_FILE, "Defaults.named", "busy", 4, DART2_FILE,
                  "Status.busy", "Variable", 5, "enum Status { ready, busy }"),
        SL_USE_AT("head-of-a-default", DART2_FILE, "Defaults.named", "Status", 9, DART2_FILE,
                  "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("default-beside-a-parameter-of-another-name", DART2_FILE,
                  "Defaults.noParameterRead", "ready", 5, DART2_FILE, "Status.ready", "Variable", 5,
                  "enum Status { ready, busy }"),
        SL_USE_AT("head-of-that-default", DART2_FILE, "Defaults.noParameterRead", "Status", 11,
                  DART2_FILE, "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("default-of-a-positional-parameter", DART2_FILE, "main.positional", "busy", 5,
                  DART2_FILE, "Status.busy", "Variable", 5, "enum Status { ready, busy }"),
        SL_USE_AT("head-of-that-positional-default", DART2_FILE, "main.positional", "Status", 13,
                  DART2_FILE, "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("default-of-a-constructor-parameter", "lib/ctor.dart", "lib.ctor", "ready", 0,
                  DART2_FILE, "Status.ready", "Variable", 5, "enum Status { ready, busy }"),
        SL_USE_AT("head-of-that-constructor-default", "lib/ctor.dart", "lib.ctor", "Status", 0,
                  DART2_FILE, "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("type-of-a-field-beside-it", "lib/ctor.dart", "lib.ctor", "Status", 1, DART2_FILE,
                  "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("a-class-header-reads-its-base", "lib/header.dart", "lib.header", "Base", 0,
                  DART2_BASE_FILE, "base.Base", "Class", 1, "abstract class Base<T> {"),
        SL_USE_AT("a-class-header-reads-a-type-argument", "lib/header.dart", "lib.header", "Status", 0,
                  DART2_FILE, "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("a-class-header-reads-its-mixin", "lib/header.dart", "lib.header", "Helper", 0,
                  DART2_BASE_FILE, "base.Helper", "Class", 9, "mixin Helper on Base<int> {"),
        SL_USE_AT("a-class-header-reads-its-interface", "lib/header.dart", "lib.header", "Marker", 0,
                  DART2_BASE_FILE, "base.Marker", "Class", 13, "class Marker {}"),
        SL_USE_AT("type-of-a-top-level-variable", "lib/top.dart", "lib.top", "Status", 0, DART2_FILE,
                  "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("head-of-its-value", "lib/top.dart", "lib.top", "Status", 1, DART2_FILE,
                  "main.Status", "Enum", 5, "enum Status { ready, busy }"),
        SL_USE_AT("its-value", "lib/top.dart", "lib.top", "ready", 0, DART2_FILE, "Status.ready",
                  "Variable", 5, "enum Status { ready, busy }"),
        DART2_CALL("path-dependency-selects-its-package", "pathDep", "sharedFn()", 0,
                   "packages/one/lib/shared.dart", "shared.sharedFn", "Function", 2,
                   "void sharedFn() { extraFn(); }"),
        SL_CALL_AT("another-pubspec-selects-the-other-package", "packages/three/lib/three.dart",
                   "three.callsTwo", "sharedFn()", 0, "packages/two/lib/shared.dart",
                   "shared.sharedFn", "Function", 2, "void sharedFn() { extraFn(); }"),
        SL_CALL_AT("a-package-resolves-its-own-name", "packages/two/lib/shared.dart",
                   "shared.sharedFn", "extraFn()", 0, "packages/two/lib/extra.dart",
                   "extra.extraFn", "Function", 1, "void extraFn() {}"),
        SL_CALL_AT("call-inside-a-mixin-member", DART2_BASE_FILE, "Helper.fromHelper", "_hidden()",
                   0, DART2_BASE_FILE, "Helper._hidden", "Method", 11, "  void _hidden() {}"),
        SL_CALL_AT("getter-typed-by-a-fixed-parameter", "lib/box.dart", "ItemBox.viaGetter",
                   "value.itemGo", 1, "lib/box.dart", "Item.itemGo", "Method", 8,
                   "class Item { void itemGo() {} }"),
        SL_CALL_AT("method-typed-by-a-fixed-parameter", "lib/box.dart", "ItemBox.viaMethod",
                   ".itemGo()", 2, "lib/box.dart", "Item.itemGo", "Method", 8,
                   "class Item { void itemGo() {} }"),
        SL_CALL_AT("the-inherited-method-it-goes-through", "lib/box.dart", "ItemBox.viaMethod",
                   "make()", 1, "lib/box.dart", "Box.make", "Method", 5, "  T make() => held;"),
        SL_CALL_AT("field-typed-by-a-fixed-parameter", "lib/box.dart", "ItemBox.viaField",
                   ".itemGo()", 3, "lib/box.dart", "Item.itemGo", "Method", 8,
                   "class Item { void itemGo() {} }"),
        SL_CALL_AT("parameter-fixed-two-levels-up", "lib/box.dart", "Deep.viaTwoLevels",
                   "value.itemGo", 2, "lib/box.dart", "Item.itemGo", "Method", 8,
                   "class Item { void itemGo() {} }"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("call-of-a-value-with-no-call-method", DART2_FILE, "Caller.noValueCall",
                   "plain(1)", 0),
        SL_NO_USE_AT("bare-name-is-not-an-enum-constant", DART2_FILE, "Caller.noBareConstant",
                     "ready", 2),
        SL_NO_USE_AT("unknown-receiver-reads-nothing", DART2_FILE, "Caller.noUnknownReceiver",
                     "ready", 3),
        SL_NO_USE_AT("a-parameter-shadows-the-getter-of-its-name", DART2_FILE,
                     "Defaults.noParameterRead", "shadowed", 1),
        SL_NO_USE_AT("called-member-is-not-a-read", DART2_FILE, "Caller.noCalledMember", "subGo", 1),
        SL_DENY_AT("bound-type-parameter-hides-the-class", DART2_FILE, "Bound.noTypeParam",
                   "s.subGo", 2),
        SL_DENY_AT("private-mixin-member-of-another-library", DART2_FILE, "Caller.mixinPrivate",
                   "_hidden()", 0),
        SL_DENY_AT("hosted-name-that-two-pubspecs-share", "packages/four/lib/four.dart",
                   "four.noPathDep", "sharedFn()", 0),
        SL_DENY_AT("a-type-parameter-hides-the-class-of-its-name", "lib/box.dart",
                   "Box.noOwnParameter", "value.itemGo", 0),
        SL_DENY_AT("a-parameter-left-open", "lib/box.dart", "Open.noParameterLeftOpen",
                   "value.itemGo", 3),
        SL_DENY_AT("a-generic-argument-is-not-its-element", "lib/box.dart", "Listed.noElementType",
                   "value.itemGo", 4),
    };
    ASSERT(sl_check("dart-reads", ".dart", "void filler() {}\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Scala ─────────────────────────────────────────────────────────── */

static const char SC_MAIN[] =
    "package app\n"
    "\n"
    "import lib.Util\n"
    "import lib.Wild._\n"
    "import lib.{Renames => R}\n"
    "\n"
    "class Base {\n"
    "  def fromBase(): Unit = ()\n"
    "}\n"
    "trait First {\n"
    "  def both(): Unit = ()\n"
    "}\n"
    "trait Last {\n"
    "  def both(): Unit = ()\n"
    "}\n"
    "case class Box(id: Int) {\n"
    "  def put(): Unit = ()\n"
    "  def self2: Box = this\n"
    "}\n"
    "object Box {\n"
    "  def make(): Box = Box(1)\n"
    "}\n"
    "class Plain {\n"
    "  def go(): Unit = ()\n"
    "}\n"
    "class Service(val box: Box, other: Box) extends Base with First with Last {\n"
    "  val made = new Box(2)\n"
    "  def ownM(): Unit = ()\n"
    "  def callsOwn(): Unit = ownM()\n"
    "  def callsThis(): Unit = this.ownM()\n"
    "  def callsInherited(): Unit = fromBase()\n"
    "  def callsLastTrait(): Unit = both()\n"
    "  def callsClassParam(): Unit = box.put()\n"
    "  def callsPrivateParam(): Unit = other.put()\n"
    "  def callsField(): Unit = made.put()\n"
    "  def callsParam(p: Box): Unit = p.put()\n"
    "  def callsLocal(): Unit = { val l = new Box(3); l.put() }\n"
    "  def callsTyped(): Unit = { val t: Box = Box.make(); t.put() }\n"
    "  def callsCompanion(): Unit = Box.make()\n"
    "  def callsChain(): Unit = Box.make().put()\n"
    "  def callsCaseApply(): Unit = Box(4).put()\n"
    "  def callsPlainNew(): Unit = new Plain().go()\n"
    "  def callsPackageFn(): Unit = sameOther()\n"
    "  def callsExplicitImport(): Unit = Util.help()\n"
    "  def callsWildcard(): Unit = fromWild()\n"
    "  def callsRenamed(): Unit = R.renamed()\n"
    "  def noParamShadow(ownM: () => Unit): Unit = ownM()\n"
    "  def noLambda(): Unit = List(1).map(ownM => ownM())\n"
    "  def noMatch(o: Option[Box]): Unit = o match { case Some(box) => box.put(); case None => () "
    "}\n"
    "  def noGeneric[T](t: T): Unit = t.put()\n"
    "  def noAbsent(p: Box): Unit = p.absent()\n"
    "  def noUnknownImport(): Unit = notAnywhere()\n"
    "  def localShadows(): Unit = { val box = new Plain(); box.go() }\n"
    "  def mentionsObject(): Any = Util\n"
    "  def noFieldName(p: Any): Any = p.Util\n"
    "}\n"
    "class Open extends outside.Thing {\n"
    "  def own(): Unit = ()\n"
    "  def callsOwnOpen(): Unit = own()\n"
    "  def noFromOutside(): Unit = sameOther()\n"
    "}\n";
static const char SC_OTHER[] = "package app\n"
                               "\n"
                               "def sameOther(): Unit = ()\n";
static const char SC_LIB[] = "package lib\n"
                             "\n"
                             "object Util {\n"
                             "  def help(): Unit = ()\n"
                             "}\n"
                             "object Wild {\n"
                             "  def fromWild(): Unit = ()\n"
                             "}\n"
                             "object Renames {\n"
                             "  def renamed(): Unit = ()\n"
                             "}\n";
static const char SC_OUTER[] = "package app\n"
                               "\n"
                               "import outside.Everything._\n"
                               "\n"
                               "class Shadowed {\n"
                               "  def noOtherFile(): Unit = sameOther()\n"
                               "  def sameUnit(): Unit = here()\n"
                               "  def here(): Unit = ()\n"
                               "}\n";

#define SC_FILE "src/app/Main.scala"
#define SC_CALL(id, method, text, at, tfile, tsuffix, label, tline, tdecl)                     \
    SL_CALL_AT(id, SC_FILE, "Service." method, text, at, tfile, tsuffix, label, tline, tdecl)
#define SC_DENY(id, method, text, at) SL_DENY_AT(id, SC_FILE, "Service." method, text, at)
#define SC_PUT(id, method, text)                                                               \
    SC_CALL(id, method, text, 0, SC_FILE, "Box.put", "Method", 17, "  def put(): Unit = ()")
#define SC_MAKE(id, method, at)                                                                \
    SC_CALL(id, method, "Box.make()", at, SC_FILE, "Box.make", "Method", 21,                   \
            "  def make(): Box = Box(1)")

TEST(resolver_source_scala_packages_precedence_and_receivers) {
    static const RQ_Source sources[] = {
        {.path = SC_FILE, .text = SC_MAIN},
        {.path = "src/app/Other.scala", .text = SC_OTHER},
        {.path = "src/lib/Util.scala", .text = SC_LIB},
        {.path = "src/app/Outer.scala", .text = SC_OUTER},
    };
    static const RQ_Expected expected[] = {
        SC_CALL("own-member", "callsOwn", "ownM()", 1, SC_FILE, "Service.ownM", "Method", 28,
                "  def ownM(): Unit = ()"),
        SC_CALL("this-member", "callsThis", "this.ownM()", 0, SC_FILE, "Service.ownM", "Method", 28,
                "  def ownM(): Unit = ()"),
        SC_CALL("superclass-member", "callsInherited", "fromBase()", 1, SC_FILE, "Base.fromBase",
                "Method", 8, "  def fromBase(): Unit = ()"),
        SC_CALL("last-mixed-trait-wins", "callsLastTrait", "both()", 2, SC_FILE, "Last.both",
                "Method", 14, "  def both(): Unit = ()"),
        SC_PUT("val-class-parameter", "callsClassParam", "box.put()"),
        SC_PUT("plain-class-parameter", "callsPrivateParam", "other.put()"),
        SC_PUT("field-from-new", "callsField", "made.put()"),
        SC_PUT("declared-parameter", "callsParam", "p.put()"),
        SC_CALL("new-constructs-the-class", "callsLocal", "new Box(3)", 0, SC_FILE, "Main.Box",
                "Class", 20, "object Box {"),
        SC_PUT("local-from-new", "callsLocal", "l.put()"),
        SC_MAKE("companion-method", "callsTyped", 0),
        SC_PUT("declared-local", "callsTyped", "t.put()"),
        SC_MAKE("object-method", "callsCompanion", 1),
        SC_MAKE("object-method-in-a-chain", "callsChain", 2),
        SC_PUT("declared-return-type", "callsChain", "Box.make().put()"),
        SC_CALL("case-class-apply-constructs", "callsCaseApply", "Box(4)", 0, SC_FILE, "Main.Box",
                "Class", 20, "object Box {"),
        SC_PUT("member-of-a-constructed-case-class", "callsCaseApply", "Box(4).put()"),
        SC_CALL("new-of-a-class-with-no-companion", "callsPlainNew", "new Plain()", 0, SC_FILE,
                "Main.Plain", "Class", 23, "class Plain {"),
        SC_CALL("member-after-new", "callsPlainNew", "new Plain().go()", 0, SC_FILE, "Plain.go",
                "Method", 24, "  def go(): Unit = ()"),
        SC_CALL("another-file-of-the-package", "callsPackageFn", "sameOther()", 0,
                "src/app/Other.scala", "Other.sameOther", "Function", 3,
                "def sameOther(): Unit = ()"),
        SC_CALL("explicit-import", "callsExplicitImport", "Util.help()", 0, "src/lib/Util.scala",
                "Util.help", "Method", 4, "  def help(): Unit = ()"),
        SC_CALL("wildcard-of-a-project-object", "callsWildcard", "fromWild()", 0,
                "src/lib/Util.scala", "Wild.fromWild", "Method", 7, "  def fromWild(): Unit = ()"),
        SC_CALL("renamed-import", "callsRenamed", "R.renamed()", 0, "src/lib/Util.scala",
                "Renames.renamed", "Method", 10, "  def renamed(): Unit = ()"),
        SL_USE_AT("object-read-as-a-value", SC_FILE, "Service.mentionsObject", "Util", 2,
                  "src/lib/Util.scala", "Util.Util", "Class", 3, "object Util {"),
        SC_CALL("new-beside-a-shadowing-local", "localShadows", "new Plain()", 1, SC_FILE,
                "Main.Plain", "Class", 23, "class Plain {"),
        SC_CALL("local-shadows-parameter-with-its-own-type", "localShadows", "box.go()", 0, SC_FILE,
                "Plain.go", "Method", 24, "  def go(): Unit = ()"),
        SL_CALL_AT("own-member-under-an-outside-parent", SC_FILE, "Open.callsOwnOpen", "own()", 1,
                   SC_FILE, "Open.own", "Method", 58, "  def own(): Unit = ()"),
        SL_CALL_AT("own-unit-beats-an-outside-wildcard", "src/app/Outer.scala", "Shadowed.sameUnit",
                   "here()", 0, "src/app/Outer.scala", "Shadowed.here", "Method", 8,
                   "  def here(): Unit = ()"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("field-name-is-not-a-read", SC_FILE, "Service.noFieldName", "Util", 3),
        SC_DENY("parameter-shadows-member", "noParamShadow", "ownM()", 3),
        SC_DENY("lambda-parameter-shadows-member", "noLambda", "ownM()", 4),
        SC_DENY("case-pattern-shadows-parameter", "noMatch", "box.put()", 1),
        SC_DENY("generic-parameter", "noGeneric", "t.put()", 1),
        SC_DENY("absent-may-be-an-implicit-conversion", "noAbsent", "p.absent()", 0),
        SC_DENY("name-nowhere-in-project", "noUnknownImport", "notAnywhere()", 0),
        SL_DENY_AT("package-member-under-an-outside-parent", SC_FILE, "Open.noFromOutside",
                   "sameOther()", 1),
        SL_DENY_AT("outside-wildcard-outranks-another-file", "src/app/Outer.scala",
                   "Shadowed.noOtherFile", "sameOther()", 0),
    };
    ASSERT(sl_check("scala-rules", ".scala", "package filler\n\nobject Filler\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Scala: positions read, types written in other files ───────────── */

static const char SC2_LIB[] =
    "package lib\n"
    "\n"
    "import lib.deep.Imported\n"
    "import external.Elsewhere\n"
    "\n"
    "class Base {\n"
    "  def inherited(a: Int): Int = a\n"
    "  val limit: Int = 1\n"
    "}\n"
    "class FromImport extends Imported\n"
    "class FromOutside extends Elsewhere\n"
    "class Made {\n"
    "  def imported(): Imported = new Imported\n"
    "  def outside(): Elsewhere = null\n"
    "  def fresh = new Thing\n"
    "  def freshBlock = { val a = 1; new Thing }\n"
    "  def noMethod(a: Int) = new Thing\n"
    "}\n"
    "class Thing {\n"
    "  def go(): Int = 1\n"
    "  def gen[T](a: T): T = a\n"
    "  def +(other: Thing): Thing = this\n"
    "  def merge(other: Thing): Thing = this\n"
    "  def ::(other: Thing): Thing = this\n"
    "  def self: Thing = this\n"
    "  val mine: Int = 1\n"
    "  var counted: Int = 2\n"
    "}\n"
    "object Things {\n"
    "  val Shared: Int = 3\n"
    "  def empty: Thing = new Thing\n"
    "}\n"
    "object Mode {\n"
    "  case object Dev\n"
    "}\n"
    "class Elsewhere\n";
static const char SC2_DEEP[] = "package lib.deep\n"
                              "\n"
                              "class Imported {\n"
                              "  def deepMember(): Int = 1\n"
                              "}\n";
static const char SC2_APP[] =
    "package app\n"
    "\n"
    "import lib._\n"
    "import lib.deep.Imported\n"
    "\n"
    "case class Conf(a: Int = 1)\n"
    "class Host(conf: Conf = Conf(1)) extends Base {\n"
    "  def own(a: Int): Int = a\n"
    "  def own2(a: Int): Int = a\n"
    "  def shadow(a: Int): Int = a\n"
    "  val here: Int = 4\n"
    "  def viaDefault(c: Any = Conf(2)): Int = 1\n"
    "  def viaFor = for { a <- Option(own(1)); b = own(2) if b > own2(3) } yield a\n"
    "  def viaGeneric(t: Thing) = t.gen[Int](1)\n"
    "  def viaBareGeneric = make[Int](1)\n"
    "  def make[T](a: T): T = a\n"
    "  def viaInfix(a: Thing, b: Thing) = a merge b\n"
    "  def viaSymbol(a: Thing, b: Thing) = a + b\n"
    "  def viaRight(a: Thing, b: Thing) = a :: b\n"
    "  def viaInfixResult(a: Thing, b: Thing) = (a + b).go()\n"
    "  def viaImportedReturn(m: Made) = m.imported().deepMember()\n"
    "  def viaImportedBase(f: FromImport) = f.deepMember()\n"
    "  def viaInferredDef(m: Made) = m.fresh.go()\n"
    "  def viaInferredBlock(m: Made) = m.freshBlock.go()\n"
    "  def viaInferredCall(m: Made) = m.noMethod(1).go()\n"
    "  def viaEmpty = Things.empty.go()\n"
    "  def viaSelf(t: Thing) = t.self.go()\n"
    "  def thing(): Thing = new Thing\n"
    "  def readsMember = thing().mine\n"
    "  def readsVar = thing().counted\n"
    "  def readsObjectValue = Things.Shared\n"
    "  def readsCaseObject = Mode.Dev\n"
    "  def readsOwnValue = here\n"
    "  def readsInherited = limit\n"
    "  def readsTypePath(a: lib.deep.Imported): Int = 1\n"
    "  def noOutsideReturn(m: Made) = m.outside().deepMember()\n"
    "  def noOutsideBase(f: FromOutside) = f.deepMember()\n"
    "  def noMethodWithArgs(m: Made) = m.noMethod.go()\n"
    "  def noInfixAbsent(a: Thing, b: Thing) = a absent b\n"
    "  def noInfixUnknown(a: String, b: Thing) = a merge b\n"
    "  def noLocalMember(mine: Any) = mine\n"
    "  def noForBinding = for { here <- Option(1) } yield here\n"
    "  def noMemberOfOutside(s: String) = s.mine\n"
    "  def noAnonymousOwn = new Thing { def shadow(a: Int): Int = a; val v = shadow(1) }\n"
    "  def anonymousInherits = new Thing { val g = go() }\n"
    "  def noAnonymousOutside = new Elsewhere2 { val x = shadow(1) }\n"
    "  def anonymousArgument = new Thing2(shadow(2)) { }\n"
    "  def noCalledRead = thing().go()\n"
    "  def assignsNamed = own(a = 1)\n"
    "  def viaTypedPattern(x: Any) = x match { case e: Thing => e.go(); case _ => 0 }\n"
    "  def viaTypedLambda(l: Seq[Thing]) = l.map((y: Thing) => y.go())\n"
    "  def noUntypedLambda(l: Seq[Thing]) = l.map(z => z.go())\n"
    "}\n"
    "class Thing2(a: Int)\n";
static const char SC2_SUITE[] = "package suite\n"
                                "\n"
                                "trait Common {\n"
                                "  trait Tester {\n"
                                "    def run(): Int = 1\n"
                                "  }\n"
                                "  def helper(): Int = 2\n"
                                "}\n"
                                "class Impl extends Common {\n"
                                "  def make = new Tester {\n"
                                "    val v = helper()\n"
                                "    val w = run()\n"
                                "  }\n"
                                "  def typed(t: Tester) = t.run()\n"
                                "}\n";
static const char SC2_SHADOW[] = "package app\n"
                                "\n"
                                "class Imported {\n"
                                "  def deepMember(): Int = 2\n"
                                "}\n"
                                "class Local {\n"
                                "  def readsDecoyPath(a: lib.deep.Imported): Int = 1\n"
                                "}\n";
#define SC2_FILE "src/app/Host.scala"
#define SC2_LIB_FILE "src/lib/Lib.scala"
#define SC2_DEEP_FILE "src/lib/deep/Deep.scala"
#define SC2_CALL(id, method, text, tfile, tsuffix, label, tline, tdecl)                         \
    SL_CALL_AT(id, SC2_FILE, "Host." method, text, 0, tfile, tsuffix, label, tline, tdecl)
#define SC2_GO(id, method, text)                                                                \
    SC2_CALL(id, method, text, SC2_LIB_FILE, "Thing.go", "Method", 20, "  def go(): Int = 1")
#define SC2_DEEP_MEMBER(id, method, text)                                                       \
    SC2_CALL(id, method, text, SC2_DEEP_FILE, "Imported.deepMember", "Method", 4,                \
            "  def deepMember(): Int = 1")
#define SC2_DENY(id, method, text) SL_DENY_AT(id, SC2_FILE, "Host." method, text, 0)

TEST(resolver_source_scala_positions_and_types_written_in_other_files) {
    static const RQ_Source sources[] = {
        {.path = SC2_FILE, .text = SC2_APP},
        {.path = SC2_LIB_FILE, .text = SC2_LIB},
        {.path = SC2_DEEP_FILE, .text = SC2_DEEP},
        {.path = "src/app/Shadow.scala", .text = SC2_SHADOW},
        {.path = "src/suite/Suite.scala", .text = SC2_SUITE},
    };
    static const RQ_Expected expected[] = {
        SC2_CALL("default-of-a-function-parameter", "viaDefault", "Conf(2)", SC2_FILE, "app.Host.Conf",
                "Class", 6, "case class Conf(a: Int = 1)"),
        SC2_CALL("generator-of-a-for", "viaFor", "own(1)", SC2_FILE, "Host.own", "Method", 8,
                "  def own(a: Int): Int = a"),
        SC2_CALL("value-of-a-for", "viaFor", "own(2)", SC2_FILE, "Host.own", "Method", 8,
                "  def own(a: Int): Int = a"),
        SC2_CALL("guard-of-a-for", "viaFor", "own2(3)", SC2_FILE, "Host.own2", "Method", 9,
                "  def own2(a: Int): Int = a"),
        SC2_CALL("generic-member-call", "viaGeneric", "t.gen[Int](1)", SC2_LIB_FILE, "Thing.gen",
                "Method", 21, "  def gen[T](a: T): T = a"),
        SC2_CALL("generic-bare-call", "viaBareGeneric", "make[Int](1)", SC2_FILE, "Host.make",
                "Method", 16, "  def make[T](a: T): T = a"),
        SC2_CALL("named-infix", "viaInfix", "a merge b", SC2_LIB_FILE, "Thing.merge", "Method", 23,
                "  def merge(other: Thing): Thing = this"),
        SC2_CALL("symbolic-infix", "viaSymbol", "a + b", SC2_LIB_FILE, "Thing.+", "Method", 22,
                "  def +(other: Thing): Thing = this"),
        SC2_CALL("right-associative-infix", "viaRight", "a :: b", SC2_LIB_FILE, "Thing.::", "Method",
                24, "  def ::(other: Thing): Thing = this"),
        SL_CALL_AT("enclosing-template-from-its-anonymous-class", "src/suite/Suite.scala",
                   "Impl.make", "helper()", 1, "src/suite/Suite.scala", "Common.helper", "Method", 7,
                   "  def helper(): Int = 2"),
        SL_CALL_AT("member-of-an-inherited-type-in-its-anonymous-class", "src/suite/Suite.scala",
                   "Impl.make", "run()", 1, "src/suite/Suite.scala", "Tester.run", "Method", 5,
                   "    def run(): Int = 1"),
        SL_CALL_AT("parameter-of-an-inherited-type", "src/suite/Suite.scala", "Impl.typed", "t.run()",
                   0, "src/suite/Suite.scala", "Tester.run", "Method", 5, "    def run(): Int = 1"),
        SC2_GO("result-of-an-infix", "viaInfixResult", "(a + b).go()"),
        SC2_GO("pattern-says-its-type", "viaTypedPattern", "e.go()"),
        SL_CALL_AT("lambda-parameter-says-its-type", SC2_FILE, "Host.viaTypedLambda", "y.go()", 1,
                   SC2_LIB_FILE, "Thing.go", "Method", 20, "  def go(): Int = 1"),
        SC2_DEEP_MEMBER("return-type-imported-in-its-file", "viaImportedReturn",
                       "m.imported().deepMember()"),
        SC2_DEEP_MEMBER("base-class-imported-in-its-file", "viaImportedBase", "f.deepMember()"),
        SC2_GO("def-of-a-new", "viaInferredDef", "m.fresh.go()"),
        SC2_GO("def-of-a-block-ending-in-new", "viaInferredBlock", "m.freshBlock.go()"),
        SC2_GO("call-of-a-def-of-a-new", "viaInferredCall", "m.noMethod(1).go()"),
        SC2_GO("method-without-arguments-is-called", "viaEmpty", "Things.empty.go()"),
        SC2_GO("member-without-arguments-is-called", "viaSelf", "t.self.go()"),
        SL_CALL_AT("anonymous-class-inherits-its-base", SC2_FILE, "Host.anonymousInherits", "go()",
                   7, SC2_LIB_FILE, "Thing.go", "Method", 20, "  def go(): Int = 1"),
        SC2_CALL("new-of-an-anonymous-class", "anonymousInherits", "new Thing { val g = go() }",
                 SC2_LIB_FILE, "lib.Lib.Thing", "Class", 19, "class Thing {"),
        SC2_CALL("argument-of-an-anonymous-class-is-outside-it", "anonymousArgument", "shadow(2)",
                 SC2_FILE, "Host.shadow", "Method", 10, "  def shadow(a: Int): Int = a"),
        SC2_CALL("new-with-an-argument-and-a-body", "anonymousArgument",
                 "new Thing2(shadow(2)) { }", SC2_FILE, "app.Host.Thing2", "Class", 54,
                 "class Thing2(a: Int)"),
        SC2_CALL("receiver-of-an-imported-return", "viaImportedReturn", "m.imported()", SC2_LIB_FILE,
                 "Made.imported", "Method", 13, "  def imported(): Imported = new Imported"),
        SC2_CALL("receiver-of-a-def-of-a-new", "viaInferredCall", "m.noMethod(1)", SC2_LIB_FILE,
                 "Made.noMethod", "Method", 17, "  def noMethod(a: Int) = new Thing"),
        SL_CALL_AT("infix-as-a-receiver", SC2_FILE, "Host.viaInfixResult", "a + b", 1, SC2_LIB_FILE,
                   "Thing.+", "Method", 22, "  def +(other: Thing): Thing = this"),
        SL_USE_AT("value-member-through-a-receiver", SC2_FILE, "Host.readsMember", "mine", 0,
                  SC2_LIB_FILE, "Lib.Thing.mine", "Variable", 26, "  val mine: Int = 1"),
        SL_USE_AT("var-member-through-a-receiver", SC2_FILE, "Host.readsVar", "counted", 0,
                  SC2_LIB_FILE, "Lib.Thing.counted", "Variable", 27, "  var counted: Int = 2"),
        SC2_CALL("new-of-an-anonymous-class-with-its-own-member", "noAnonymousOwn",
                 "new Thing { def shadow(a: Int): Int = a; val v = shadow(1) }", SC2_LIB_FILE,
                 "lib.Lib.Thing", "Class", 19, "class Thing {"),
        SC2_CALL("receiver-of-an-outside-return", "noOutsideReturn", "m.outside()", SC2_LIB_FILE,
                 "Made.outside", "Method", 14, "  def outside(): Elsewhere = null"),
        SL_USE_AT("own-value-by-bare-name", SC2_FILE, "Host.readsOwnValue", "here", 1, SC2_FILE,
                  "Host.here", "Variable", 11, "  val here: Int = 4"),
        SL_USE_AT("inherited-value-by-bare-name", SC2_FILE, "Host.readsInherited", "limit", 0,
                  SC2_LIB_FILE, "Lib.Base.limit", "Variable", 8, "  val limit: Int = 1"),
        SL_USE_AT("a-type-path-names-its-own-type", SC2_FILE, "Host.readsTypePath", "Imported", 3,
                  SC2_DEEP_FILE, "deep.Deep.Imported", "Class", 3, "class Imported {"),
        {.case_id = "a-type-path-is-a-type-use",
         .site = {.file = SC2_FILE,
                  .caller = "Host.readsTypePath",
                  .needle = "lib.deep.Imported",
                  .occurrence_index = 1},
         .relation = "USES_TYPE",
         .target = {.file = SC2_DEEP_FILE,
                    .suffix = "deep.Deep.Imported",
                    .label = "Class",
                    .declaration_line = 3,
                    .declaration_text = "class Imported {"}},
    };
    static const RQ_Forbidden forbidden[] = {
        SC2_DENY("untyped-lambda-parameter", "noUntypedLambda", "z.go()"),
        SC2_DENY("return-type-from-an-outside-import", "noOutsideReturn",
                "m.outside().deepMember()"),
        SL_DENY_AT("base-class-from-an-outside-import", SC2_FILE, "Host.noOutsideBase",
                   "f.deepMember()", 1),
        SC2_DENY("a-method-with-arguments-is-no-receiver", "noMethodWithArgs", "m.noMethod.go()"),
        SC2_DENY("infix-member-absent", "noInfixAbsent", "a absent b"),
        SL_DENY_AT("infix-on-an-unknown-type", SC2_FILE, "Host.noInfixUnknown", "a merge b", 1),
        SL_DENY_AT("bare-call-in-an-anonymous-class-with-an-outside-base", SC2_FILE,
                   "Host.noAnonymousOutside", "shadow(1)", 1),
        SL_NO_USE_AT("parameter-hides-a-member", SC2_FILE, "Host.noLocalMember", "mine", 2),
        SL_NO_USE_AT("for-binding-hides-a-member", SC2_FILE, "Host.noForBinding", "here", 3),
        SL_NO_USE_AT("member-of-an-outside-type", SC2_FILE, "Host.noMemberOfOutside", "mine", 3),
        SC2_DENY("own-member-of-an-anonymous-class", "noAnonymousOwn", "shadow(1)"),
        SL_NO_USE_AT("a-called-member-is-not-also-a-read", SC2_FILE, "Host.noCalledRead", "go", 8),
    };
    ASSERT(sl_check("scala-positions", ".scala", "package f\nobject F\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Haskell ───────────────────────────────────────────────────────── */

static const char HS_MAIN[] = "module Main where\n"
                              "\n"
                              "import Data.Maybe (fromMaybe)\n"
                              "import Lib (exported)\n"
                              "import qualified Lib as L\n"
                              "import Other hiding (hidden)\n"
                              "\n"
                              "local :: Int -> Int\n"
                              "local x = x\n"
                              "\n"
                              "callsLocal :: Int\n"
                              "callsLocal = local 1\n"
                              "\n"
                              "callsImported :: Int\n"
                              "callsImported = exported 1\n"
                              "\n"
                              "callsQualified :: Int\n"
                              "callsQualified = L.exported 2\n"
                              "\n"
                              "callsOpen :: Int\n"
                              "callsOpen = open 3\n"
                              "\n"
                              "usesValue :: [Int]\n"
                              "usesValue = map local [1]\n"
                              "\n"
                              "noShadow :: (Int -> Int) -> Int\n"
                              "noShadow local = local 4\n"
                              "\n"
                              "noOutside :: Int\n"
                              "noOutside = fromMaybe 1 Nothing\n"
                              "\n"
                              "noHidden :: Int\n"
                              "noHidden = hidden 5\n"
                              "\n"
                              "noNotListed :: Int\n"
                              "noNotListed = notListed 6\n";
static const char HS_LIB[] = "module Lib (exported) where\n"
                             "\n"
                             "exported :: Int -> Int\n"
                             "exported x = x\n"
                             "\n"
                             "notListed :: Int -> Int\n"
                             "notListed x = x\n";
static const char HS_OTHER[] = "module Other where\n"
                               "\n"
                               "open :: Int -> Int\n"
                               "open x = x\n"
                               "\n"
                               "hidden :: Int -> Int\n"
                               "hidden x = x\n";

#define HS_FILE "src/Main.hs"
#define HS_CALL(id, method, text, tfile, tsuffix, tline, tdecl)                                \
    SL_CALL_AT(id, HS_FILE, "Main." method, text, 0, tfile, tsuffix, "Function", tline, tdecl)
#define HS_DENY(id, method, text) SL_DENY_AT(id, HS_FILE, "Main." method, text, 0)

TEST(resolver_source_haskell_imports_and_scope) {
    static const RQ_Source sources[] = {
        {.path = HS_FILE, .text = HS_MAIN},
        {.path = "src/Lib.hs", .text = HS_LIB},
        {.path = "src/Other.hs", .text = HS_OTHER},
    };
    static const RQ_Expected expected[] = {
        HS_CALL("top-level-function", "callsLocal", "local 1", HS_FILE, "Main.local", 9,
                "local x = x"),
        HS_CALL("name-in-an-import-list", "callsImported", "exported 1", "src/Lib.hs",
                "Lib.exported", 4, "exported x = x"),
        HS_CALL("qualified-alias", "callsQualified", "L.exported 2", "src/Lib.hs", "Lib.exported",
                4, "exported x = x"),
        HS_CALL("open-import", "callsOpen", "open 3", "src/Other.hs", "Other.open", 4,
                "open x = x"),
        SL_USE_AT("function-read-as-a-value", HS_FILE, "Main.usesValue", "local", 3, HS_FILE,
                  "Main.local", "Function", 9, "local x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        HS_DENY("parameter-shadows-the-function", "noShadow", "local 4"),
        SL_NO_USE_AT("parameter-read-is-not-the-function", HS_FILE, "Main.noShadow", "local", 5),
        HS_DENY("module-outside-the-project", "noOutside", "fromMaybe 1 Nothing"),
        HS_DENY("hiding-clause-removes-it", "noHidden", "hidden 5"),
        HS_DENY("not-in-the-export-list", "noNotListed", "notListed 6"),
    };
    ASSERT(sl_check("haskell-rules", ".hs", "module Filler where\n\nf :: Int\nf = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── OCaml ─────────────────────────────────────────────────────────── */

static const char ML_MAIN[] = "module L = Lib\n"
                              "let local x = x\n"
                              "let calls_local () = local 1\n"
                              "let calls_module () = Lib.exported 2\n"
                              "let calls_alias () = L.exported 3\n"
                              "let uses_value () = List.map local [1]\n"
                              "let no_shadow local = local 4\n"
                              "let no_after () = later 5\n"
                              "let later x = x\n"
                              "let no_outside () = Printf.printf \"x\"\n"
                              "open Other\n"
                              "let calls_open () = opened 6\n"
                              "let no_replaced () = local 7\n"
                              "module Lib = struct\n"
                              "  let exported = Lib.exported\n"
                              "end\n";

#define ML_FILE "src/main.ml"
#define ML_CALL(id, method, text, tfile, tsuffix, tline, tdecl)                                \
    SL_CALL_AT(id, ML_FILE, "main." method, text, 0, tfile, tsuffix, "Function", tline, tdecl)
#define ML_DENY(id, method, text) SL_DENY_AT(id, ML_FILE, "main." method, text, 0)

TEST(resolver_source_ocaml_sequential_scope_and_modules) {
    static const RQ_Source sources[] = {
        {.path = ML_FILE, .text = ML_MAIN},
        {.path = "src/lib.ml", .text = "let exported x = x\n"},
        {.path = "src/other.ml", .text = "let opened x = x\nlet local x = x\n"},
    };
    static const RQ_Expected expected[] = {
        ML_CALL("earlier-let", "calls_local", "local 1", ML_FILE, "main.local", 2,
                "let local x = x"),
        ML_CALL("sibling-compilation-unit", "calls_module", "Lib.exported 2", "src/lib.ml",
                "lib.exported", 1, "let exported x = x"),
        ML_CALL("module-alias", "calls_alias", "L.exported 3", "src/lib.ml", "lib.exported", 1,
                "let exported x = x"),
        ML_CALL("open-of-a-known-unit", "calls_open", "opened 6", "src/other.ml", "other.opened", 1,
                "let opened x = x"),
        SL_USE_AT("module-is-not-in-scope-in-its-own-body", ML_FILE, "main.Lib.exported",
                  "Lib.exported", 1, "src/lib.ml", "lib.exported", "Function", 1,
                  "let exported x = x"),
        SL_USE_AT("function-read-as-a-value", ML_FILE, "main.uses_value", "local", 3, ML_FILE,
                  "main.local", "Function", 2, "let local x = x"),
    };
    static const RQ_Forbidden forbidden[] = {
        ML_DENY("parameter-shadows-the-let", "no_shadow", "local 4"),
        SL_NO_USE_AT("parameter-read-is-not-the-let", ML_FILE, "main.no_shadow", "local", 5),
        ML_DENY("let-is-not-yet-in-scope", "no_after", "later 5"),
        ML_DENY("module-outside-the-project", "no_outside", "Printf.printf \"x\""),
        ML_DENY("a-later-open-replaces-the-name", "no_replaced", "local 7"),
    };
    ASSERT(sl_check("ocaml-rules", ".ml", "let filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Lua ───────────────────────────────────────────────────────────── */

/* Forms from neovim: `local clear = n.clear`, `require('test.functional.testnvim')()`, a module
 * table returned from a `lua/` root, `Screen.new` building an instance through `__index`, and
 * LuaCATS stubs declaring `vim.api.*` in one global namespace. */
static const char LUA_HELPER[] = "local M = {}\n"
                                 "\n"
                                 "function M.greet(name)\n"
                                 "  return name\n"
                                 "end\n"
                                 "\n"
                                 "M.assigned = function() end\n"
                                 "\n"
                                 "function M:method()\n"
                                 "  return self:sibling()\n"
                                 "end\n"
                                 "\n"
                                 "function M:sibling()\n"
                                 "  return M.greet('from sibling')\n"
                                 "end\n"
                                 "\n"
                                 "local function hidden() end\n"
                                 "\n"
                                 "M.renamed = hidden\n"
                                 "\n"
                                 "M.conflict = hidden\n"
                                 "M.conflict = M.greet\n"
                                 "\n"
                                 "M.format = function() end\n"
                                 "\n"
                                 "return M\n";
static const char LUA_UTIL[] = "local M = {\n"
                               "  eq = function() end,\n"
                               "}\n"
                               "\n"
                               "function M.ok() end\n"
                               "\n"
                               "return M\n";
static const char LUA_FACTORY[] = "local M = {}\n"
                                  "\n"
                                  "function M.clear() end\n"
                                  "\n"
                                  "return function()\n"
                                  "  return M\n"
                                  "end\n";
static const char LUA_CLASS[] = "local Base = require('lib.base')\n"
                                "\n"
                                "local Class = setmetatable({}, { __index = Base })\n"
                                "Class.__index = Class\n"
                                "\n"
                                "function Class.new()\n"
                                "  local self = setmetatable({}, Class)\n"
                                "  return self\n"
                                "end\n"
                                "\n"
                                "function Class:run()\n"
                                "  self:inherited()\n"
                                "  return self\n"
                                "end\n"
                                "\n"
                                "return Class\n";
static const char LUA_BASE[] = "local Base = {}\n"
                               "\n"
                               "function Base:inherited() end\n"
                               "\n"
                               "return Base\n";
static const char LUA_STUBS[] = "vim.api = {}\n"
                                "\n"
                                "function vim.api.nvim_thing() end\n";
static const char LUA_SHARED[] = "function shared_global() end\n"
                                 "\n"
                                 "function vim.keycode(text)\n"
                                 "  return text\n"
                                 "end\n"
                                 "\n"
                                 "function usage() end\n"
                                 "\n"
                                 "function print(...) end\n";
static const char LUA_OTHER[] = "function usage() end\n"
                                "\n"
                                "function own_script()\n"
                                "  usage()\n"
                                "end\n";
static const char LUA_MAIN[] =
    "local h = require('lib.helper')\n"
    "local t = require('test.util')\n"
    "local n = require('test.factory')()\n"
    "local Class = require('lib.class')\n"
    "local greet = h.greet\n"
    "local helper_mod = h\n"
    "local a, second_ok = h.greet, t.ok\n"
    "local api = vim.api\n"
    "local counter = 0\n"
    "\n"
    "local function own() end\n"
    "\n"
    "local function caller()\n"
    "  h.greet('x')\n"
    "  greet('y')\n"
    "  second_ok(1)\n"
    "  n.clear()\n"
    "  own()\n"
    "  h:method()\n"
    "  require('lib.helper').greet('z')\n"
    "  local c = Class.new()\n"
    "  c:run()\n"
    "  c:inherited()\n"
    "  shared_global()\n"
    "  vim.api.nvim_thing()\n"
    "  api.nvim_thing()\n"
    "  vim.keycode('k')\n"
    "  t.eq(1)\n"
    "end\n"
    "\n"
    "local function reads()\n"
    "  return counter, helper_mod.renamed, second_ok\n"
    "end\n"
    "\n"
    "local function shadow(greet, own)\n"
    "  greet('p')\n"
    "  own()\n"
    "  local counter = 1\n"
    "  return counter\n"
    "end\n"
    "\n"
    "local function rewritten()\n"
    "  local f = h.greet\n"
    "  f = h.assigned\n"
    "  f()\n"
    "end\n"
    "\n"
    "local function not_proved()\n"
    "  h.missing()\n"
    "  h.conflict()\n"
    "  string.format('x')\n"
    "  print('x')\n"
    "  usage()\n"
    "  require('ext.lib').greet()\n"
    "end\n"
    "\n"
    "local function replaced_environment()\n"
    "  local _ENV = { shared_global = own }\n"
    "  shared_global()\n"
    "end\n"
    "\n"
    "local function scoped()\n"
    "  do\n"
    "    local inner = greet\n"
    "    inner('in')\n"
    "  end\n"
    "  inner('out')\n"
    "end\n"
    "\n"
    "local Wrapped = {}\n"
    "local before = Wrapped.go\n"
    "function Wrapped.go()\n"
    "  before()\n"
    "end\n"
    "\n"
    "local function with_default(opts)\n"
    "  opts = opts or {}\n"
    "  local setup = opts.setup\n"
    "  function opts.setup()\n"
    "    setup()\n"
    "  end\n"
    "end\n"
    "\n"
    "local function preloaded()\n"
    "  require('os').shadowed_by_a_project_file()\n"
    "end\n"
    "\n"
    "local function own_require(require)\n"
    "  local z = require('lib.helper')\n"
    "  z.greet('s')\n"
    "end\n"
    "\n"
    "return { caller = caller }\n";

#define LUA_HELPER_FILE "lua/lib/helper.lua"
#define LUA_FILE "test/main_spec.lua"
#define LUA_CALL(id, text, tfile, tsuffix, tline, tdecl)                                       \
    SL_CALL_AT(id, LUA_FILE, "main_spec.caller", text, 0, tfile, tsuffix, "Function", tline, tdecl)
#define LUA_DENY(id, method, text) SL_DENY_AT(id, LUA_FILE, "main_spec." method, text, 0)

TEST(resolver_source_lua_values_modules_and_scope) {
    static const RQ_Source sources[] = {
        {.path = LUA_FILE, .text = LUA_MAIN},
        {.path = LUA_HELPER_FILE, .text = LUA_HELPER},
        {.path = "test/util.lua", .text = LUA_UTIL},
        {.path = "test/factory.lua", .text = LUA_FACTORY},
        {.path = "lua/lib/class.lua", .text = LUA_CLASS},
        {.path = "lua/lib/base.lua", .text = LUA_BASE},
        {.path = "meta/api.lua", .text = LUA_STUBS},
        {.path = "shared.lua", .text = LUA_SHARED},
        {.path = "other_script.lua", .text = LUA_OTHER},
        {.path = "os.lua", .text = "local M = {}\nfunction M.shadowed_by_a_project_file() end\nreturn M\n"},
    };
    static const RQ_Expected expected[] = {
        LUA_CALL("member-of-a-required-table", "h.greet('x')", LUA_HELPER_FILE, "helper.M.greet", 3,
                 "function M.greet(name)"),
        LUA_CALL("local-holding-a-member", "greet('y')", LUA_HELPER_FILE, "helper.M.greet", 3,
                 "function M.greet(name)"),
        LUA_CALL("second-name-of-a-local-statement", "second_ok(1)", "test/util.lua", "util.M.ok", 5,
                 "function M.ok() end"),
        LUA_CALL("value-a-required-function-returns", "n.clear()", "test/factory.lua",
                 "factory.M.clear", 3, "function M.clear() end"),
        SL_CALL_AT("local-function", LUA_FILE, "main_spec.caller", "own()", 1, LUA_FILE, "main_spec.own",
                   "Function", 11, "local function own() end"),
        LUA_CALL("method-of-a-required-table", "h:method()", LUA_HELPER_FILE, "helper.M:method", 9,
                 "function M:method()"),
        LUA_CALL("member-of-a-require-expression", "require('lib.helper').greet('z')",
                 LUA_HELPER_FILE, "helper.M.greet", 3, "function M.greet(name)"),
        LUA_CALL("constructor-function", "Class.new()", "lua/lib/class.lua", "class.Class.new", 6,
                 "function Class.new()"),
        LUA_CALL("method-of-an-instance", "c:run()", "lua/lib/class.lua", "class.Class:run", 11,
                 "function Class:run()"),
        LUA_CALL("metatable-index-chain", "c:inherited()", "lua/lib/base.lua",
                 "base.Base:inherited", 3, "function Base:inherited() end"),
        LUA_CALL("global-function-of-another-file", "shared_global()", "shared.lua",
                 "shared.shared_global", 1, "function shared_global() end"),
        LUA_CALL("global-table-member", "vim.api.nvim_thing()", "meta/api.lua",
                 "api.vim.api.nvim_thing", 3, "function vim.api.nvim_thing() end"),
        SL_CALL_AT("local-holding-a-global-table", LUA_FILE, "main_spec.caller", "api.nvim_thing()", 1,
                   "meta/api.lua", "api.vim.api.nvim_thing", "Function", 3,
                   "function vim.api.nvim_thing() end"),
        LUA_CALL("function-in-a-global-table", "vim.keycode('k')", "shared.lua",
                 "shared.vim.keycode", 3, "function vim.keycode(text)"),
        SL_CALL_AT("self-method", LUA_HELPER_FILE, "helper.M:method", "self:sibling()", 0,
                   LUA_HELPER_FILE, "helper.M:sibling", "Function", 13, "function M:sibling()"),
        SL_CALL_AT("inner-block-local", LUA_FILE, "main_spec.scoped", "inner('in')", 0,
                   LUA_HELPER_FILE, "helper.M.greet", "Function", 3, "function M.greet(name)"),
        SL_CALL_AT("own-file-global-before-another-file", "other_script.lua",
                   "other_script.own_script", "usage()", 1, "other_script.lua",
                   "other_script.usage", "Function", 1, "function usage() end"),
        SL_USE_AT("block-local-reads-the-chunk-alias", LUA_FILE, "main_spec.scoped", "greet", 10,
                  LUA_FILE, "main_spec.greet", "Variable", 5, "local greet = h.greet"),
        SL_USE_AT("chunk-level-local-read", LUA_FILE, "main_spec.reads", "counter", 1, LUA_FILE,
                  "main_spec.counter", "Variable", 9, "local counter = 0"),
        SL_USE_AT("alias-local-read", LUA_FILE, "main_spec.reads", "helper_mod", 1, LUA_FILE,
                  "main_spec.helper_mod", "Variable", 6, "local helper_mod = h"),
        SL_USE_AT("second-name-has-a-node", LUA_FILE, "main_spec.reads", "second_ok", 2, LUA_FILE,
                  "main_spec.second_ok", "Variable", 7, "local a, second_ok = h.greet, t.ok"),
        SL_USE_AT("member-read-holding-a-function", LUA_FILE, "main_spec.reads", "renamed", 0,
                  LUA_HELPER_FILE, "helper.hidden", "Function", 17, "local function hidden() end"),
    };
    static const RQ_Forbidden forbidden[] = {
        LUA_DENY("parameter-shadows-the-alias", "shadow", "greet('p')"),
        SL_DENY_AT("parameter-shadows-the-function", LUA_FILE, "main_spec.shadow", "own()", 2),
        SL_NO_USE_AT("inner-local-is-not-the-chunk-local", LUA_FILE, "main_spec.shadow", "counter",
                     1),
        LUA_DENY("a-local-written-with-another-value", "rewritten", "f()"),
        SL_DENY_AT("function-in-a-table-field-has-no-node", LUA_FILE, "main_spec.caller", "t.eq(1)", 0),
        LUA_DENY("member-nothing-declares", "not_proved", "h.missing()"),
        LUA_DENY("member-with-two-declarations", "not_proved", "h.conflict()"),
        LUA_DENY("standard-library-is-not-a-same-named-member", "not_proved", "string.format('x')"),
        LUA_DENY("standard-global-is-not-another-files-wrapper", "not_proved", "print('x')"),
        LUA_DENY("global-two-files-declare", "not_proved", "usage()"),
        LUA_DENY("module-outside-the-project", "not_proved", "require('ext.lib').greet()"),
        LUA_DENY("local-environment-replaces-globals", "replaced_environment", "shared_global()"),
        LUA_DENY("name-past-its-block-is-a-global", "scoped", "inner('out')"),
        LUA_DENY("parameter-named-require", "own_require", "z.greet('s')"),
        SL_DENY_AT("read-above-the-declaration-is-the-old-value", LUA_FILE, "main_spec.Wrapped.go",
                   "before()", 0),
        SL_DENY_AT("parameter-default-is-not-its-only-value", LUA_FILE, "main_spec.opts.setup",
                   "setup()", 0),
        LUA_DENY("library-the-host-preloads", "preloaded",
                 "require('os').shadowed_by_a_project_file()"),
    };
    ASSERT(sl_check("lua-rules", ".lua", "local filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* LuaCATS is what a Lua project states about a value it cannot show: a parameter, a return. An
 * annotation is about the next line. The rest of neovim's runtime reads this way:
 * `--- @param client vim.lsp.Client`, `vim.uv = ...` under `---@type uv`. */
static const char LUA_THING[] = "--- @class lib.Thing\n"
                                "local Thing = {}\n"
                                "Thing.__index = Thing\n"
                                "\n"
                                "function Thing:run() end\n"
                                "\n"
                                "--- @return lib.Thing\n"
                                "function Thing.make()\n"
                                "  return mystery()\n"
                                "end\n"
                                "\n"
                                "return Thing\n";
static const char LUA_ANNOTATED[] = "local Thing = require('lib.thing')\n"
                                    "\n"
                                    "--- @param t lib.Thing\n"
                                    "local function annotated(t)\n"
                                    "  t:run()\n"
                                    "end\n"
                                    "\n"
                                    "--- @param s string\n"
                                    "local function not_a_class(s)\n"
                                    "  s:run()\n"
                                    "end\n"
                                    "\n"
                                    "local function no_annotation(u)\n"
                                    "  u:run()\n"
                                    "end\n"
                                    "\n"
                                    "--- @class lib.Shape\n"
                                    "\n"
                                    "local Unrelated = {}\n"
                                    "function Unrelated.run() end\n"
                                    "\n"
                                    "--- @param o lib.Shape\n"
                                    "local function class_not_adjacent(o)\n"
                                    "  o.run()\n"
                                    "end\n"
                                    "\n"
                                    "local function via_return()\n"
                                    "  Thing.make():run()\n"
                                    "end\n"
                                    "\n"
                                    "local other = mystery() --- @type lib.Thing\n"
                                    "local function trailing()\n"
                                    "  other:run()\n"
                                    "end\n"
                                    "\n"
                                    "---@type lib.Thing\n"
                                    "shared_thing = ...\n"
                                    "local function typed_global()\n"
                                    "  shared_thing:run()\n"
                                    "end\n";

TEST(resolver_source_lua_annotations_state_what_a_value_cannot) {
    static const RQ_Source sources[] = {
        {.path = "lua/lib/thing.lua", .text = LUA_THING},
        {.path = "use.lua", .text = LUA_ANNOTATED},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("param-annotation", "use.lua", "use.annotated", "t:run()", 0,
                   "lua/lib/thing.lua", "thing.Thing:run", "Function", 5, "function Thing:run() end"),
        SL_CALL_AT("function-whose-return-is-annotated", "use.lua", "use.via_return", "Thing.make()",
                   0, "lua/lib/thing.lua", "thing.Thing.make", "Function", 8,
                   "function Thing.make()"),
        SL_CALL_AT("return-annotation", "use.lua", "use.via_return", "Thing.make():run()", 0,
                   "lua/lib/thing.lua", "thing.Thing:run", "Function", 5, "function Thing:run() end"),
        SL_CALL_AT("trailing-type-annotation", "use.lua", "use.trailing", "other:run()", 0,
                   "lua/lib/thing.lua", "thing.Thing:run", "Function", 5, "function Thing:run() end"),
        SL_CALL_AT("type-annotation-on-an-assignment", "use.lua", "use.typed_global",
                   "shared_thing:run()", 0, "lua/lib/thing.lua", "thing.Thing:run", "Function", 5,
                   "function Thing:run() end"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("annotation-naming-no-class", "use.lua", "use.not_a_class", "s:run()", 0),
        SL_DENY_AT("no-annotation", "use.lua", "use.no_annotation", "u:run()", 0),
        SL_DENY_AT("class-annotation-across-a-blank-line", "use.lua", "use.class_not_adjacent",
                   "o.run()", 0),
    };
    ASSERT(sl_check("lua-annotations", ".lua", "local filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* Forms from neovim: a lazy `__index` over `require(prefix .. key)`, `@module` fields of the table
 * `vim._defer_require` is given, and a test's `local function` inside `describe(...)`. A mock in a
 * test body does not compete with the declaration, and a factory's new table keeps its own writes. */
static const char LUA_LAZY[] =
    "local lazy = setmetatable({}, {\n"
    "  __index = function(t, key)\n"
    "    t[key] = require('lazylib.' .. key)\n"
    "    return t[key]\n"
    "  end,\n"
    "})\n"
    "local function make_deferred() return {} end\n"
    "local deferred = make_deferred('pkg', {\n"
    "  buf = ..., --- @module 'pkg.buf'\n"
    "})\n"
    "\n"
    "local function make()\n"
    "  return {}\n"
    "end\n"
    "local first = make()\n"
    "function first.only_first() end\n"
    "local second = make()\n"
    "function second.only_second() end\n"
    "\n"
    "function global_api() end\n"
    "\n"
    "local function callers()\n"
    "  lazy.sub.go()\n"
    "  deferred.buf.format()\n"
    "  first.only_first()\n"
    "  global_api()\n"
    "end\n"
    "\n"
    "local function no_proof()\n"
    "  lazy.absent.go()\n"
    "  deferred.other.format()\n"
    "  first.only_second()\n"
    "end\n"
    "\n"
    "describe('suite', function()\n"
    "  local function nested_helper() end\n"
    "  local function nested_caller()\n"
    "    nested_helper()\n"
    "  end\n"
    "end)\n";

TEST(resolver_source_lua_lazy_modules_factories_and_nested_functions) {
    static const RQ_Source sources[] = {
        {.path = "lazy.lua", .text = LUA_LAZY},
        {.path = "lazylib/sub.lua", .text = "local M = {}\nfunction M.go() end\nreturn M\n"},
        {.path = "pkg/buf.lua", .text = "local M = {}\nfunction M.format() end\nreturn M\n"},
        {.path = "pkg/other.lua", .text = "local M = {}\nfunction M.format() end\nreturn M\n"},
        {.path = "mock_spec.lua", .text = "local function mocks()\n"
                                          "  global_api = function() end\n"
                                          "end\n"},
        {.path = "global_user.lua", .text = "local function use_global()\n"
                                            "  global_api()\n"
                                            "end\n"},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("lazy-index-requires-the-key", "lazy.lua", "lazy.callers", "lazy.sub.go()", 0,
                   "lazylib/sub.lua", "sub.M.go", "Function", 2, "function M.go() end"),
        SL_CALL_AT("module-annotated-field", "lazy.lua", "lazy.callers", "deferred.buf.format()", 0,
                   "pkg/buf.lua", "buf.M.format", "Function", 2, "function M.format() end"),
        SL_CALL_AT("write-through-a-factory-local", "lazy.lua", "lazy.callers", "first.only_first()",
                   1, "lazy.lua", "lazy.first.only_first", "Function", 16,
                   "function first.only_first() end"),
        SL_CALL_AT("own-file-global", "lazy.lua", "lazy.callers", "global_api()", 1, "lazy.lua",
                   "lazy.global_api", "Function", 20, "function global_api() end"),
        SL_CALL_AT("mock-in-a-function-does-not-compete", "global_user.lua",
                   "global_user.use_global", "global_api()", 0, "lazy.lua", "lazy.global_api",
                   "Function", 20, "function global_api() end"),
        SL_CALL_AT("local-function-inside-a-callback", "lazy.lua", "lazy.nested_caller",
                   "nested_helper()", 1, "lazy.lua", "lazy.nested_helper", "Function", 36,
                   "  local function nested_helper() end"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("lazy-module-outside-the-project", "lazy.lua", "lazy.no_proof",
                   "lazy.absent.go()", 0),
        SL_DENY_AT("field-the-annotation-does-not-name", "lazy.lua", "lazy.no_proof",
                   "deferred.other.format()", 0),
        SL_DENY_AT("write-through-the-other-factory-local", "lazy.lua", "lazy.no_proof",
                   "first.only_second()", 0),
    };
    ASSERT(sl_check("lua-lazy", ".lua", "local filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* `@class C: A, B` states what a value it cannot compute holds, as neovim's test helper does with
 * `M = vim.tbl_extend('error', M, t_global)`. One parent declaring the name selects it; two, or a
 * parent the project has no table for, leave it open. `return a, b` gives `local x, y = f()` its y
 * when every return agrees and no last call can give more. */
static const char LUA_LATER_VALUES[] = "local merged = require('merged')\n"
                                       "local open_parent = require('open_parent')\n"
                                       "local generic = require('generic')\n"
                                       "\n"
                                       "local function pair()\n"
                                       "  local function first_value() end\n"
                                       "  local function second_value() end\n"
                                       "  return first_value, second_value\n"
                                       "end\n"
                                       "\n"
                                       "local function failing()\n"
                                       "  local function failure_path_value() end\n"
                                       "  if unknown_flag then\n"
                                       "    return nil\n"
                                       "  end\n"
                                       "  return 1, failure_path_value\n"
                                       "end\n"
                                       "\n"
                                       "local function disagree()\n"
                                       "  local function a() end\n"
                                       "  local function b() end\n"
                                       "  if unknown_flag then\n"
                                       "    return 1, a\n"
                                       "  end\n"
                                       "  return 1, b\n"
                                       "end\n"
                                       "\n"
                                       "local function forwards()\n"
                                       "  local function c() end\n"
                                       "  local function third() end\n"
                                       "  if unknown_flag then\n"
                                       "    return c, nil, third\n"
                                       "  end\n"
                                       "  return c, pair()\n"
                                       "end\n"
                                       "\n"
                                       "local one, two = pair()\n"
                                       "local f1, f2 = failing()\n"
                                       "local d1, d2 = disagree()\n"
                                       "local g1, g2, g3 = forwards()\n"
                                       "\n"
                                       "local function callers()\n"
                                       "  merged.base_fn()\n"
                                       "  merged.extra_fn()\n"
                                       "  merged.own_fn()\n"
                                       "  generic.base_fn()\n"
                                       "  two()\n"
                                       "  f2()\n"
                                       "  g2()\n"
                                       "end\n"
                                       "\n"
                                       "local function no_proof()\n"
                                       "  merged.both()\n"
                                       "  open_parent.base_fn()\n"
                                       "  d2()\n"
                                       "  g3()\n"
                                       "end\n";

TEST(resolver_source_lua_class_parents_and_later_return_values) {
    static const RQ_Source sources[] = {
        {.path = "use.lua", .text = LUA_LATER_VALUES},
        {.path = "base.lua",
         .text = "--- @class base.mod\n"
                 "local M = {}\n"
                 "function M.base_fn() end\n"
                 "function M.both() end\n"
                 "return M\n"},
        {.path = "extra.lua",
         .text = "--- @class extra.mod\n"
                 "local M = {}\n"
                 "function M.extra_fn() end\n"
                 "function M.both() end\n"
                 "return M\n"},
        {.path = "merged.lua",
         .text = "local function own_fn() end\n"
                 "--- @class merged.own\n"
                 "local M = {\n"
                 "  own_fn = own_fn,\n"
                 "}\n"
                 "--- @class merged.mod: merged.own, base.mod, extra.mod\n"
                 "M = merge(M)\n"
                 "return M\n"},
        {.path = "open_parent.lua",
         .text = "--- @class open.mod: base.mod, not.in.project\n"
                 "local M = {}\n"
                 "return M\n"},
        {.path = "generic.lua",
         .text = "--- @class (private, exact) gen.child<T, U> : base.mod\n"
                 "local M = {}\n"
                 "return M\n"},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("merged-module-first-parent", "use.lua", "use.callers", "merged.base_fn()", 0,
                   "base.lua", "base.M.base_fn", "Function", 3, "function M.base_fn() end"),
        SL_CALL_AT("merged-module-second-parent", "use.lua", "use.callers", "merged.extra_fn()", 0,
                   "extra.lua", "extra.M.extra_fn", "Function", 3, "function M.extra_fn() end"),
        SL_CALL_AT("class-table-the-write-replaced", "use.lua", "use.callers", "merged.own_fn()", 0,
                   "merged.lua", "merged.own_fn", "Function", 1, "local function own_fn() end"),
        SL_CALL_AT("parent-of-a-class-with-its-own-table", "use.lua", "use.callers",
                   "generic.base_fn()", 0, "base.lua", "base.M.base_fn", "Function", 3,
                   "function M.base_fn() end"),
        SL_CALL_AT("second-return-value", "use.lua", "use.callers", "two()", 0, "use.lua",
                   "second_value", "Function", 7, "  local function second_value() end"),
        SL_CALL_AT("nil-failure-path-does-not-compete", "use.lua", "use.callers", "f2()", 0,
                   "use.lua", "failure_path_value", "Function", 12,
                   "  local function failure_path_value() end"),
        SL_CALL_AT("call-in-last-place-gives-its-first", "use.lua", "use.callers", "g2()", 0,
                   "use.lua", "first_value", "Function", 6, "  local function first_value() end"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("two-parents-declare-the-name", "use.lua", "use.no_proof", "merged.both()", 0),
        SL_DENY_AT("parent-the-project-does-not-have", "use.lua", "use.no_proof",
                   "open_parent.base_fn()", 0),
        SL_DENY_AT("returns-disagree-on-the-position", "use.lua", "use.no_proof", "d2()", 0),
        SL_DENY_AT("position-past-a-last-call", "use.lua", "use.no_proof", "g3()", 0),
    };
    ASSERT(sl_check("lua-later", ".lua", "local filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* The effects that once voided a whole file. None of them can rebind a local: `load` and
 * `dofile` run elsewhere, and `_G.x = v` declares a global. What the manual says can change a
 * binding does abstain: setfenv for free names, debug.setlocal for a local's value. */
static const char LUA_EFFECTS[] = "local function target() return 1 end\n"
                                  "debug.getregistry()\n"
                                  "load('return nil')\n"
                                  "rawset(_G, 'other', nil)\n"
                                  "loadfile('unknown.lua')\n"
                                  "dofile('unknown.lua')\n"
                                  "loadstring('return nil')\n"
                                  "local version = _G._VERSION\n"
                                  "local function caller(...) goto done ::done:: return target() end\n"
                                  "_G.declared = function() end\n"
                                  "local function reads_g() return declared() end\n";
static const char LUA_SETFENV[] = "function free_name() return 1 end\n"
                                  "setfenv(1, {})\n"
                                  "function caller_free() return free_name() end\n";
static const char LUA_DEBUG[] = "local function f() return 1 end\n"
                                "local g = f\n"
                                "debug.setlocal(1, 1, nil)\n"
                                "local function caller_debug() return g() end\n";

TEST(resolver_source_lua_effects_that_can_change_a_binding) {
    static const RQ_Source sources[] = {
        {.path = "effects.lua", .text = LUA_EFFECTS},
        {.path = "setfenv.lua", .text = LUA_SETFENV},
        {.path = "writes_local.lua", .text = LUA_DEBUG},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("local-under-dynamic-effects", "effects.lua", "effects.caller", "target()", 1,
                   "effects.lua", "effects.target", "Function", 1,
                   "local function target() return 1 end"),
        SL_CALL_AT("global-declared-through-G", "effects.lua", "effects.reads_g", "declared()", 0,
                   "effects.lua", "effects._G.declared", "Function", 10,
                   "_G.declared = function() end"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("setfenv-moves-free-names", "setfenv.lua", "setfenv.caller_free", "free_name()",
                   0),
        SL_DENY_AT("debug-setlocal-writes-locals", "writes_local.lua", "writes_local.caller_debug",
                   "g()", 0),
    };
    ASSERT(sl_check("lua-effects", ".lua", "local filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Vim script ────────────────────────────────────────────────────── */

/* Forms from neovim's runtime and test/old/testdir. */
static const char VIM_MAIN[] = "function! s:Local() abort\n"
                               "  return 1\n"
                               "endfunction\n"
                               "function! s:Caller() abort\n"
                               "  call s:Local()\n"
                               "  call <SID>Local()\n"
                               "  call foo#bar#baz(1)\n"
                               "  call Global(2)\n"
                               "  call Shared(3)\n"
                               "  return 0\n"
                               "endfunction\n"
                               "function! Global(x) abort\n"
                               "  return a:x\n"
                               "endfunction\n"
                               "function! s:NoProof(F) abort\n"
                               "  call get(a:F, 'k')\n"
                               "  call s:Missing()\n"
                               "  call Both(1)\n"
                               "  call Holder()\n"
                               "  call a:F()\n"
                               "  call wrong#place#fn()\n"
                               "endfunction\n"
                               "let Holder = function('Global')\n"
                               "let s:dict = {}\n"
                               "function s:dict.get() abort\n"
                               "endfunction\n";
static const char VIM_SHARED[] = "function! Shared(x) abort\n"
                                 "  return a:x\n"
                                 "endfunction\n"
                                 "function! Both(x) abort\n"
                                 "endfunction\n"
                                 "function! Holder() abort\n"
                                 "endfunction\n"
                                 "function! s:Missing() abort\n"
                                 "endfunction\n"
                                 "function! wrong#place#fn() abort\n"
                                 "endfunction\n";
static const char VIM_OTHER[] = "function! Both(x) abort\n"
                                "endfunction\n";
static const char VIM_AUTOLOAD[] = "function! foo#bar#baz(x) abort\n"
                                   "  return s:Inner(a:x)\n"
                                   "endfunction\n"
                                   "function! s:Inner(x) abort\n"
                                   "  return a:x\n"
                                   "endfunction\n";

#define VIM_FILE "plugin/main.vim"
#define VIM_CALL(id, text, at, tfile, tsuffix, tline, tdecl)                                   \
    SL_CALL_AT(id, VIM_FILE, "main.s:Caller", text, at, tfile, tsuffix, "Function", tline, tdecl)
#define VIM_DENY(id, text) SL_DENY_AT(id, VIM_FILE, "main.s:NoProof", text, 0)

TEST(resolver_source_vim_name_prefixes_select_the_namespace) {
    static const RQ_Source sources[] = {
        {.path = VIM_FILE, .text = VIM_MAIN},
        {.path = "plugin/shared.vim", .text = VIM_SHARED},
        {.path = "plugin/other.vim", .text = VIM_OTHER},
        {.path = "autoload/foo/bar.vim", .text = VIM_AUTOLOAD},
    };
    static const RQ_Expected expected[] = {
        VIM_CALL("script-local", "s:Local()", 1, VIM_FILE, "main.s:Local", 1,
                 "function! s:Local() abort"),
        VIM_CALL("sid-spelling", "<SID>Local()", 0, VIM_FILE, "main.s:Local", 1,
                 "function! s:Local() abort"),
        VIM_CALL("autoload-path", "foo#bar#baz(1)", 0, "autoload/foo/bar.vim", "bar.foo#bar#baz", 1,
                 "function! foo#bar#baz(x) abort"),
        VIM_CALL("global-of-this-file", "Global(2)", 0, VIM_FILE, "main.Global", 12,
                 "function! Global(x) abort"),
        VIM_CALL("global-of-one-other-file", "Shared(3)", 0, "plugin/shared.vim", "shared.Shared", 1,
                 "function! Shared(x) abort"),
        SL_CALL_AT("script-local-of-an-autoload-file", "autoload/foo/bar.vim", "bar.foo#bar#baz",
                   "s:Inner(a:x)", 0, "autoload/foo/bar.vim", "bar.s:Inner", "Function", 4,
                   "function! s:Inner(x) abort"),
    };
    static const RQ_Forbidden forbidden[] = {
        VIM_DENY("lower-case-is-a-builtin-not-a-dict-function", "get(a:F, 'k')"),
        VIM_DENY("script-local-of-another-file", "s:Missing()"),
        VIM_DENY("global-two-files-declare", "Both(1)"),
        VIM_DENY("name-assigned-a-funcref", "Holder()"),
        VIM_DENY("parameter-funcref", "a:F()"),
        VIM_DENY("autoload-name-outside-its-path", "wrong#place#fn()"),
    };
    ASSERT(sl_check("vim-rules", ".vim", "let g:filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Tcl ───────────────────────────────────────────────────────────── */

/* Forms from redis's tests. `expr` syntax is a parse error in this grammar, and is in most files. */
static const char TCL_MAIN[] = "proc local_proc {a} {\n"
                               "    return $a\n"
                               "}\n"
                               "proc caller {} {\n"
                               "    local_proc 1\n"
                               "    wait_for_it 2\n"
                               "    set x [wait_for_it 3]\n"
                               "    harness_proc 31\n"
                               "    return [expr ($x+1) / 2]\n"
                               "}\n"
                               "proc no_proof {} {\n"
                               "    both 4\n"
                               "    standalone_only 5\n"
                               "    puts 6\n"
                               "    $computed 7\n"
                               "    renamed 8\n"
                               "}\n"
                               "namespace eval ns {\n"
                               "    proc outer {} {\n"
                               "        sibling 9\n"
                               "        wait_for_it 10\n"
                               "    }\n"
                               "    proc sibling {x} {\n"
                               "        return $x\n"
                               "    }\n"
                               "}\n";
static const char TCL_HELPER[] = "source tests/support/util.tcl\n"
                                 "source tests/support/other.tcl\n"
                                 "proc harness_proc {x} {\n"
                                 "}\n"
                                 "proc run_test {name} {\n"
                                 "    source tests/$name.tcl\n"
                                 "}\n";
static const char TCL_UTIL[] = "proc wait_for_it {x} {\n"
                               "    return $x\n"
                               "}\n"
                               "proc both {} {\n"
                               "}\n"
                               "proc renamed {x} {\n"
                               "}\n"
                               "rename renamed hidden\n";
static const char TCL_OTHER[] = "proc both {} {\n"
                                "}\n";
static const char TCL_STANDALONE[] = "proc standalone_only {x} {\n"
                                     "}\n";

#define TCL_FILE "tests/unit/main.tcl"
#define TCL_DENY(id, text) SL_DENY_AT(id, TCL_FILE, "main.no_proof", text, 0)

TEST(resolver_source_tcl_procs_and_shared_interpreter) {
    static const RQ_Source sources[] = {
        {.path = TCL_FILE, .text = TCL_MAIN},
        {.path = "tests/helper.tcl", .text = TCL_HELPER},
        {.path = "tests/support/util.tcl", .text = TCL_UTIL},
        {.path = "tests/support/other.tcl", .text = TCL_OTHER},
        {.path = "tools/standalone.tcl", .text = TCL_STANDALONE},
    };
    static const RQ_Expected expected[] = {
        SL_CALL_AT("proc-of-this-script", TCL_FILE, "main.caller", "local_proc 1", 0, TCL_FILE,
                   "main.local_proc", "Function", 1, "proc local_proc {a} {"),
        SL_CALL_AT("proc-of-a-sourced-file", TCL_FILE, "main.caller", "wait_for_it 2", 0,
                   "tests/support/util.tcl", "util.wait_for_it", "Function", 1,
                   "proc wait_for_it {x} {"),
        SL_CALL_AT("command-substitution", TCL_FILE, "main.caller", "wait_for_it 3", 0,
                   "tests/support/util.tcl", "util.wait_for_it", "Function", 1,
                   "proc wait_for_it {x} {"),
        SL_CALL_AT("proc-of-the-harness-that-loads-it", TCL_FILE, "main.caller", "harness_proc 31", 0,
                   "tests/helper.tcl", "helper.harness_proc", "Function", 3,
                   "proc harness_proc {x} {"),
    };
    static const RQ_Forbidden forbidden[] = {
        TCL_DENY("two-sourced-files-declare-it", "both 4"),
        TCL_DENY("file-nothing-sources", "standalone_only 5"),
        TCL_DENY("builtin-command", "puts 6"),
        TCL_DENY("computed-command", "$computed 7"),
        TCL_DENY("renamed-proc", "renamed 8"),
    };
    ASSERT(sl_check("tcl-rules", ".tcl", "set filler 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Shell ─────────────────────────────────────────────────────────── */

static const char SH_MAIN[] = "#!/bin/bash\n"
                              "source \"$(dirname \"$0\")/lib.sh\"\n"
                              "source scripts/literal.sh\n"
                              "ROOT=$(dirname \"${BASH_SOURCE[0]}\")/..\n"
                              "source \"${ROOT}/hack/init.sh\"\n"
                              "early\n"
                              "helper() {\n"
                              "  later 1\n"
                              "  lib_fn 2\n"
                              "  literal_fn 3\n"
                              "  init_fn 31\n"
                              "}\n"
                              "later() {\n"
                              "  helper 4\n"
                              "}\n"
                              "early() {\n"
                              "  :\n"
                              "}\n"
                              "no_proof() {\n"
                              "  other_script_fn 5\n"
                              "  gone 6\n"
                              "  $computed 7\n"
                              "  command later 8\n"
                              "}\n"
                              "gone() {\n"
                              "  :\n"
                              "}\n"
                              "unset -f gone\n"
                              "early 9\n"
                              "TOP_LEVEL=1\n"
                              "reads() {\n"
                              "  echo \"$TOP_LEVEL\"\n"
                              "}\n"
                              "SHADOWED=1\n"
                              "reads_local() {\n"
                              "  local SHADOWED=2\n"
                              "  echo \"$SHADOWED\"\n"
                              "}\n";
static const char SH_LIB[] = "lib_fn() {\n"
                             "  :\n"
                             "}\n";
static const char SH_INIT[] = "init_fn() {\n"
                              "  :\n"
                              "}\n";
static const char SH_LITERAL[] = "literal_fn() {\n"
                                 "  :\n"
                                 "}\n";
static const char SH_OTHER[] = "other_script_fn() {\n"
                               "  :\n"
                               "}\n"
                               "lib_fn() {\n"
                               "  :\n"
                               "}\n";

#define SH_FILE "scripts/main.sh"

TEST(resolver_source_shell_function_table_of_one_process) {
    static const RQ_Source sources[] = {
        {.path = SH_FILE, .text = SH_MAIN},
        {.path = "scripts/lib.sh", .text = SH_LIB},
        {.path = "scripts/literal.sh", .text = SH_LITERAL},
        {.path = "hack/init.sh", .text = SH_INIT},
        {.path = "tools/other.sh", .text = SH_OTHER},
    };
    static const RQ_Expected expected[] = {
        SL_USE_AT("variable-of-this-file", SH_FILE, "main.reads", "TOP_LEVEL", 1, SH_FILE,
                  "main.TOP_LEVEL", "Variable", 30, "TOP_LEVEL=1"),
        SL_CALL_AT("body-calls-a-later-definition", SH_FILE, "main.helper", "later 1", 0, SH_FILE,
                   "main.later", "Function", 13, "later() {"),
        SL_CALL_AT("sourced-through-the-scripts-directory", SH_FILE, "main.helper", "lib_fn 2", 0,
                   "scripts/lib.sh", "lib.lib_fn", "Function", 1, "lib_fn() {"),
        SL_CALL_AT("sourced-by-a-literal-path", SH_FILE, "main.helper", "literal_fn 3", 0,
                   "scripts/literal.sh", "literal.literal_fn", "Function", 1, "literal_fn() {"),
        SL_CALL_AT("top-level-after-the-definition", SH_FILE, "main", "early 9", 0, SH_FILE,
                   "main.early", "Function", 16, "early() {"),
        SL_CALL_AT("sourced-through-a-variable-naming-a-directory", SH_FILE, "main.helper",
                   "init_fn 31", 0, "hack/init.sh", "init.init_fn", "Function", 1, "init_fn() {"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("name-a-function-makes-local", SH_FILE, "main.reads_local", "SHADOWED", 2),
        SL_DENY_AT("top-level-before-the-definition", SH_FILE, "main", "early\n", 0),
        SL_DENY_AT("another-script-is-another-process", SH_FILE, "main.no_proof",
                   "other_script_fn 5", 0),
        SL_DENY_AT("unset-function", SH_FILE, "main.no_proof", "gone 6", 0),
        SL_DENY_AT("computed-command", SH_FILE, "main.no_proof", "$computed 7", 0),
        SL_DENY_AT("command-builtin-skips-the-table", SH_FILE, "main.no_proof", "command later 8",
                   0),
    };
    ASSERT(sl_check("shell-rules", ".sh", "filler=1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Make ──────────────────────────────────────────────────────────── */

/* From redis's deps/lua/Makefile. */
static const char MAKE_MAIN[] = "INSTALL_TOP= /usr/local\n"
                                "INSTALL_BIN= $(INSTALL_TOP)/bin\n"
                                "FROM_ENV_COPY= $(FROM_ENV)\n"
                                "COMPUTED= $($(INSTALL_TOP)_FLAGS)\n"
                                "SIBLING= sibling-only\n";

TEST(resolver_source_make_variables_of_the_file) {
    static const RQ_Source sources[] = {
        {.path = "deps/Makefile", .text = MAKE_MAIN},
        {.path = "other/Makefile", .text = "FROM_ENV= elsewhere\n"},
    };
    static const RQ_Expected expected[] = {
        SL_USE_AT("variable-assigned-in-this-file", "deps/Makefile", "Makefile",
                  "$(INSTALL_TOP)", 0, "deps/Makefile", "Makefile.INSTALL_TOP", "Variable", 1,
                  "INSTALL_TOP= /usr/local"),
        SL_USE_AT("read-inside-a-computed-name", "deps/Makefile", "Makefile", "$(INSTALL_TOP)", 1,
                  "deps/Makefile", "Makefile.INSTALL_TOP", "Variable", 1,
                  "INSTALL_TOP= /usr/local"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("variable-of-another-makefile", "deps/Makefile", "Makefile",
                     "$(FROM_ENV)", 0),
    };
    ASSERT(sl_check("make-rules", ".mk", "FILLER= 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

/* ── Vue ───────────────────────────────────────────────────────────── */

/* koel's SFC: the template's directives, the style block and the two scripts do not stop the
 * script from being a module, and a member of an exported object literal is selected across
 * files by the rule the literal's own file uses. */
/* A composable's `return { a }`, as Vue and React code read it: `const { a } = useThing()`.
 * Every return must agree, a spread takes what is before it, and what is not a function of this
 * project selects nothing. */
static const char COMPOSABLE[] = "const outside = () => {}\n"
                                 "export const useThing = () => {\n"
                                 "  const open = () => {}\n"
                                 "  function declared() {}\n"
                                 "  let written = () => {}\n"
                                 "  written = () => {}\n"
                                 "  return {\n"
                                 "    open,\n"
                                 "    declared,\n"
                                 "    renamed: open,\n"
                                 "    written,\n"
                                 "    outside,\n"
                                 "    inline: () => {},\n"
                                 "    computed: open.bind(null),\n"
                                 "  }\n"
                                 "}\n"
                                 "export function useSpread() {\n"
                                 "  const before = () => {}\n"
                                 "  const after = () => {}\n"
                                 "  return { before, ...other(), after }\n"
                                 "}\n"
                                 "export function useTwoReturns(flag) {\n"
                                 "  const one = () => {}\n"
                                 "  const two = () => {}\n"
                                 "  const same = () => {}\n"
                                 "  if (flag) return { picked: one, same }\n"
                                 "  return { picked: two, same }\n"
                                 "}\n"
                                 "export function useValue() {\n"
                                 "  const hidden = () => {}\n"
                                 "  if (other()) return { hidden }\n"
                                 "  return makeObject()\n"
                                 "}\n"
                                 "export const useExpression = () => ({ made: outside })\n";

static const char COMPOSABLE_USER[] =
    "<script setup>\n"
    "import { useThing, useSpread, useTwoReturns, useValue, useExpression } from './composable'\n"
    "const { open, declared, renamed, written, outside, inline, computed } = useThing()\n"
    "const { open: alias } = useThing()\n"
    "const { before, after } = useSpread()\n"
    "const { picked, same } = useTwoReturns(true)\n"
    "const { hidden } = useValue()\n"
    "const { made } = useExpression()\n"
    "let { open: reassigned } = useThing()\n"
    "const { open: defaulted = () => {} } = useThing()\n"
    "const { renamed: { declared: nested_member } } = useThing()\n"
    "\n"
    "function calls() {\n"
    "  open()\n"
    "  declared()\n"
    "  renamed()\n"
    "  outside()\n"
    "  inline()\n"
    "  alias()\n"
    "  after()\n"
    "  same()\n"
    "  made()\n"
    "}\n"
    "\n"
    "function no_proof(parameter) {\n"
    "  written()\n"
    "  computed()\n"
    "  before()\n"
    "  picked()\n"
    "  hidden()\n"
    "  reassigned()\n"
    "  defaulted()\n"
    "  nested_member()\n"
    "}\n"
    "\n"
    "function shadowed(open) {\n"
    "  open()\n"
    "}\n"
    "\n"
    "function nested() {\n"
    "  const { open: inner } = useThing()\n"
    "  inner()\n"
    "}\n"
    "</script>\n";

TEST(resolver_source_vue_composable_return_selects_the_member) {
    static const RQ_Source sources[] = {
        {.path = "src/composable.ts", .text = COMPOSABLE},
        {.path = "src/Main.vue", .text = COMPOSABLE_USER},
    };
#define COMPOSABLE_USER_FILE "src/Main.vue"
#define COMPOSABLE_FILE "src/composable.ts"
    static const RQ_Expected expected[] = {
        SL_CALL_AT("shorthand-of-a-local-function", COMPOSABLE_USER_FILE, "Main.calls", "open()", 0,
                   COMPOSABLE_FILE, "open", "Function", 3, "  const open = () => {}"),
        SL_CALL_AT("shorthand-of-a-declaration", COMPOSABLE_USER_FILE, "Main.calls", "declared()", 0,
                   COMPOSABLE_FILE, "declared", "Function", 4, "  function declared() {}"),
        SL_CALL_AT("renamed-member", COMPOSABLE_USER_FILE, "Main.calls", "renamed()", 0,
                   COMPOSABLE_FILE, "open", "Function", 3, "  const open = () => {}"),
        SL_CALL_AT("module-level-function-returned", COMPOSABLE_USER_FILE, "Main.calls", "outside()",
                   0, COMPOSABLE_FILE, "outside", "Function", 1, "const outside = () => {}"),
        SL_CALL_AT("inline-function-member", COMPOSABLE_USER_FILE, "Main.calls", "inline()", 0,
                   COMPOSABLE_FILE, "inline", "Function", 13, "    inline: () => {},"),
        SL_CALL_AT("pattern-renames-the-local", COMPOSABLE_USER_FILE, "Main.calls", "alias()", 0,
                   COMPOSABLE_FILE, "open", "Function", 3, "  const open = () => {}"),
        SL_CALL_AT("after-a-spread-is-the-literals", COMPOSABLE_USER_FILE, "Main.calls", "after()",
                   0, COMPOSABLE_FILE, "after", "Function", 19, "  const after = () => {}"),
        SL_CALL_AT("every-return-agrees", COMPOSABLE_USER_FILE, "Main.calls", "same()", 0,
                   COMPOSABLE_FILE, "same", "Function", 25, "  const same = () => {}"),
        SL_CALL_AT("expression-body-object", COMPOSABLE_USER_FILE, "Main.calls", "made()", 0,
                   COMPOSABLE_FILE, "outside", "Function", 1, "const outside = () => {}"),
        SL_CALL_AT("function-level-destructure", COMPOSABLE_USER_FILE, "Main.nested", "inner()", 0,
                   COMPOSABLE_FILE, "open", "Function", 3, "  const open = () => {}"),
        SL_CALL_AT("the-composable-itself", COMPOSABLE_USER_FILE, "Main.nested", "useThing()", 5,
                   COMPOSABLE_FILE, "useThing", "Function", 2, "export const useThing = () => {"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_DENY_AT("returned-local-is-written", COMPOSABLE_USER_FILE, "Main.no_proof", "written()",
                   0),
        SL_DENY_AT("member-value-is-computed", COMPOSABLE_USER_FILE, "Main.no_proof", "computed()",
                   0),
        SL_DENY_AT("spread-can-replace-what-is-before-it", COMPOSABLE_USER_FILE, "Main.no_proof",
                   "before()", 0),
        SL_DENY_AT("returns-disagree", COMPOSABLE_USER_FILE, "Main.no_proof", "picked()", 0),
        SL_DENY_AT("another-return-is-not-an-object", COMPOSABLE_USER_FILE, "Main.no_proof",
                   "hidden()", 0),
        SL_DENY_AT("let-can-be-written", COMPOSABLE_USER_FILE, "Main.no_proof", "reassigned()", 0),
        SL_DENY_AT("default-can-be-the-value", COMPOSABLE_USER_FILE, "Main.no_proof", "defaulted()",
                   0),
        SL_DENY_AT("nested-pattern-is-a-member-of-the-member", COMPOSABLE_USER_FILE, "Main.no_proof",
                   "nested_member()", 0),
        SL_DENY_AT("parameter-shadows-the-destructured-const", COMPOSABLE_USER_FILE, "Main.shadowed",
                   "open()", 1),
    };
    ASSERT(sl_check("vue-composable", ".vue", "<script>const filler = 1</script>\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

static const char VUE_SERVICE[] = "export const authService = {\n"
                                  "  login() {\n"
                                  "    return 1\n"
                                  "  },\n"
                                  "  logout: () => 2,\n"
                                  "}\n"
                                  "\n"
                                  "export const written = {\n"
                                  "  replaced() {},\n"
                                  "}\n"
                                  "written.replaced = () => 3\n"
                                  "\n"
                                  "export function plain() {}\n"
                                  "\n"
                                  "const base = {}\n"
                                  "export const spreadFirst = {\n"
                                  "  ...base,\n"
                                  "  first() {},\n"
                                  "}\n"
                                  "export const spreadLast = {\n"
                                  "  last() {},\n"
                                  "  ...base,\n"
                                  "}\n";
static const char VUE_MAIN[] = "<template>\n"
                               "  <BaseCard v-if=\"ready\" :title=\"`x`\" @click.prevent=\"go\">\n"
                               "    {{ go() }}\n"
                               "  </BaseCard>\n"
                               "</template>\n"
                               "\n"
                               "<script lang=\"ts\">\n"
                               "export const sharedConstant = 1\n"
                               "</script>\n"
                               "\n"
                               "<script setup lang=\"ts\" generic=\"T\">\n"
                               "import { authService, written, plain, spreadFirst, spreadLast } "
                               "from './service'\n"
                               "\n"
                               "const { podcast } = defineProps<{ podcast: T }>()\n"
                               "const ready = true\n"
                               "function helper() {}\n"
                               "const reads = () => podcast\n"
                               "const reads_parameter = (podcast) => podcast\n"
                               "const go = () => {\n"
                               "  plain()\n"
                               "  helper()\n"
                               "  authService.login()\n"
                               "  authService.logout()\n"
                               "  authService.missing()\n"
                               "  written.replaced()\n"
                               "  spreadFirst.first()\n"
                               "  spreadLast.last()\n"
                               "  return sharedConstant\n"
                               "}\n"
                               "</script>\n"
                               "\n"
                               "<style scoped>\n"
                               ".a { color: red; }\n"
                               "</style>\n";

#define VUE_FILE "src/Main.vue"

TEST(resolver_source_vue_script_reaches_the_ecma_resolver) {
    static const RQ_Source sources[] = {
        {.path = VUE_FILE, .text = VUE_MAIN},
        {.path = "src/service.ts", .text = VUE_SERVICE},
    };
    static const RQ_Expected expected[] = {
        SL_USE_AT("read-of-a-destructured-const", VUE_FILE, "Main.reads", "podcast", 2, VUE_FILE,
                  "Main.podcast", "Variable", 14,
                  "const { podcast } = defineProps<{ podcast: T }>()"),
        SL_CALL_AT("imported-function", VUE_FILE, "Main.go", "plain()", 0, "src/service.ts",
                   "service.plain", "Function", 13, "export function plain() {}"),
        SL_CALL_AT("function-of-the-script", VUE_FILE, "Main.go", "helper()", 1, VUE_FILE,
                   "Main.helper", "Function", 16, "function helper() {}"),
        SL_CALL_AT("method-of-an-exported-literal", VUE_FILE, "Main.go", "authService.login()", 0,
                   "src/service.ts", "service.login", "Function", 2, "  login() {"),
        SL_CALL_AT("member-after-a-spread-is-the-literals", VUE_FILE, "Main.go", "spreadFirst.first()",
                   0, "src/service.ts", "service.first", "Function", 18, "  first() {},"),
        SL_CALL_AT("arrow-of-an-exported-literal", VUE_FILE, "Main.go", "authService.logout()", 0,
                   "src/service.ts", "service.logout", "Function", 5, "  logout: () => 2,"),
    };
    static const RQ_Forbidden forbidden[] = {
        SL_NO_USE_AT("parameter-is-not-the-destructured-const", VUE_FILE, "Main.reads_parameter",
                     "podcast", 4),
        SL_DENY_AT("member-the-literal-does-not-have", VUE_FILE, "Main.go",
                   "authService.missing()", 0),
        SL_DENY_AT("member-of-a-written-literal", VUE_FILE, "Main.go", "written.replaced()", 0),
        SL_DENY_AT("spread-can-replace-what-is-before-it", VUE_FILE, "Main.go", "spreadLast.last()", 0),
    };
    ASSERT(sl_check("vue-rules", ".ts", "export const filler = 1\n", sources,
                    sizeof(sources) / sizeof(sources[0]), expected,
                    sizeof(expected) / sizeof(expected[0]), forbidden,
                    sizeof(forbidden) / sizeof(forbidden[0])));
    PASS();
}

SUITE(resolver_source_langs) {
    RUN_TEST(resolver_source_elixir_modules_aliases_imports_and_arity);
    RUN_TEST(resolver_source_erlang_module_arity_export_and_import);
    RUN_TEST(resolver_source_zig_containers_imports_and_declared_receivers);
    RUN_TEST(resolver_source_zig_unwraps_captures_and_member_reads);
    RUN_TEST(resolver_source_zig_a_type_is_read_in_the_file_that_writes_it);
    RUN_TEST(resolver_source_scala_positions_and_types_written_in_other_files);
    RUN_TEST(resolver_source_perl_packages_exporter_and_method_resolution);
    RUN_TEST(resolver_source_ruby_ancestor_chain_and_self_rules);
    RUN_TEST(resolver_source_ruby_writers_constructors_aliases_and_constants);
    RUN_TEST(resolver_source_ruby_block_keeps_self_only_for_a_method_that_lets_it);
    RUN_TEST(resolver_source_ruby_project_each_that_rebinds_blocks_the_core_rule);
    RUN_TEST(resolver_source_ruby_constants_and_file_locals_are_references);
    RUN_TEST(resolver_source_ruby_bare_new_and_index_operators);
    RUN_TEST(resolver_source_ruby_super_selects_what_follows_the_own_step);
    RUN_TEST(resolver_source_ruby_superclass_reaches_the_graph_on_both_routes);
    RUN_TEST(resolver_source_elixir_captures_mentions_protocols_and_erlang_atoms);
    RUN_TEST(resolver_source_elixir_use_brings_what_using_returns);
    RUN_TEST(resolver_source_erlang_fun_references);
    RUN_TEST(resolver_source_perl_invocant_reexports_and_code_references);
    RUN_TEST(resolver_source_perl_list_shift_and_overload);
    RUN_TEST(resolver_source_ocaml_dune_libraries_includes_and_scoped_opens);
    RUN_TEST(resolver_source_ocaml_subdirectories_and_public_names);
    RUN_TEST(resolver_source_ocaml_a_module_is_part_of_a_functions_name);
    RUN_TEST(resolver_source_ocaml_includes_under_opens_and_interfaces);
    RUN_TEST(resolver_source_ocaml_functors_module_types_and_interfaces);
    RUN_TEST(resolver_source_haskell_constructors);
    RUN_TEST(resolver_source_haskell_class_methods);
    RUN_TEST(resolver_source_parse_damage_stays_where_it_is);
    RUN_TEST(resolver_source_dart_lexical_scope_libraries_and_receivers);
    RUN_TEST(resolver_source_swift_modules_lookup_and_receivers);
    RUN_TEST(resolver_source_swift_parse_view_reads_and_extensions);
    RUN_TEST(resolver_source_swift_implicit_members_and_overloads_by_type);
    RUN_TEST(resolver_source_swift_initializer_types_and_nested_context);
    RUN_TEST(resolver_source_swift_closures_and_value_forms);
    RUN_TEST(resolver_source_swift_a_closure_of_no_parameters);
    RUN_TEST(resolver_source_dart_headers_reads_members_and_pubspec_paths);
    RUN_TEST(resolver_source_scala_packages_precedence_and_receivers);
    RUN_TEST(resolver_source_haskell_imports_and_scope);
    RUN_TEST(resolver_source_ocaml_sequential_scope_and_modules);
    RUN_TEST(resolver_source_lua_values_modules_and_scope);
    RUN_TEST(resolver_source_lua_effects_that_can_change_a_binding);
    RUN_TEST(resolver_source_lua_annotations_state_what_a_value_cannot);
    RUN_TEST(resolver_source_lua_lazy_modules_factories_and_nested_functions);
    RUN_TEST(resolver_source_lua_class_parents_and_later_return_values);
    RUN_TEST(resolver_source_vim_name_prefixes_select_the_namespace);
    RUN_TEST(resolver_source_tcl_procs_and_shared_interpreter);
    RUN_TEST(resolver_source_shell_function_table_of_one_process);
    RUN_TEST(resolver_source_make_variables_of_the_file);
    RUN_TEST(resolver_source_vue_script_reaches_the_ecma_resolver);
    RUN_TEST(resolver_source_vue_composable_return_selects_the_member);
}
