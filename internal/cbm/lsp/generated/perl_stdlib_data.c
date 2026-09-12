/*
 * perl_stdlib_data.c — hand-written Perl stdlib + CPAN type data.
 *
 * Strategy mirrors php_stdlib_data.c (docs/PLAN_PHP_LSP_INTEGRATION.md §6):
 *   1. perlfunc core built-ins (print, bless, ref, ...) registered as global,
 *      package-less functions reachable from any namespace.
 *   2. Curated, corpus-driven CPAN OOP modules (Scalar::Util, List::Util,
 *      Carp, POSIX, Storable, Data::Dumper) registered as module-qualified
 *      functions.
 *
 * Module-qualified functions use dotted QNs (Foo.Bar.func) to match
 * perl_pkg_to_dot (Foo::Bar -> Foo.Bar) so an Exporter import map
 * (plan 22-03) can resolve `use Scalar::Util qw(blessed)` to these symbols.
 *
 * Return types are left UNKNOWN (cbm_type_unknown) for v1: real signature
 * inference is out of scope here — this seed only provides a baseline symbol
 * table for the resolver. Moose meta stubs (has/extends/with) are deferred
 * (Open Question #4).
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../perl_lsp.h"
#include <string.h>

#define MIXED cbm_type_unknown()

/* Register a global (package-less) built-in function returning `ret_type_`.
 * Reachable from any package — short_name == qualified_name (bare name). */
#define REG_BUILTIN(name_, ret_type_)                                                           \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = (name_);                                                            \
        rf.short_name = (name_);                                                                \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                                              \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

/* Register a module-qualified function (an exported sub, not a method).
 * `module_dot_` is the dotted package QN (e.g. "Scalar.Util"); `name_` is the
 * bare sub name. QN becomes "Scalar.Util.blessed"; short_name stays bare so an
 * Exporter import map can resolve `use Scalar::Util qw(blessed)`. */
#define REG_FUNC(module_dot_, name_, ret_type_)                                                 \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", (module_dot_), (name_));          \
        rf.short_name = (name_);                                                                \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                                              \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

/* Register a stdlib OO type (e.g. DBI.db) so receiver-typed method dispatch
 * has a home. Dotted QN matches perl_pkg_to_dot. */
#define REG_TYPE(qn_)                                                                           \
    do {                                                                                        \
        CBMRegisteredType rt;                                                                   \
        memset(&rt, 0, sizeof(rt));                                                             \
        rt.qualified_name = (qn_);                                                              \
        rt.short_name = strrchr((qn_), '.') ? strrchr((qn_), '.') + 1 : (qn_);                  \
        rt.is_stdlib = true;                                                                    \
        cbm_registry_add_type(reg, rt);                                                         \
    } while (0)

/* Register a method on a stdlib type via receiver_type (the pattern the
 * resolver's direct method lookup consumes). QN "Type.method". */
#define REG_METHOD(type_dot_, name_, ret_type_)                                                 \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", (type_dot_), (name_));            \
        rf.short_name = (name_);                                                                \
        rf.receiver_type = (type_dot_);                                                         \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                                              \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

void cbm_perl_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredFunc rf;

    /* ── perlfunc core built-ins (global, package-less) ─────────────
     * Source: RESEARCH.md L365 (perldoc perlfunc core list). Reachable from
     * any package; return types unknown for v1. */
    REG_BUILTIN("print", MIXED);
    REG_BUILTIN("printf", MIXED);
    REG_BUILTIN("sprintf", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("open", MIXED);
    REG_BUILTIN("close", MIXED);
    REG_BUILTIN("push", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("pop", MIXED);
    REG_BUILTIN("shift", MIXED);
    REG_BUILTIN("unshift", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("map", MIXED);
    REG_BUILTIN("grep", MIXED);
    REG_BUILTIN("sort", MIXED);
    REG_BUILTIN("join", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("split", MIXED);
    REG_BUILTIN("length", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("substr", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("chomp", MIXED);
    REG_BUILTIN("chop", MIXED);
    REG_BUILTIN("die", MIXED);
    REG_BUILTIN("warn", MIXED);
    REG_BUILTIN("ref", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("bless", MIXED);
    REG_BUILTIN("defined", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("exists", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("delete", MIXED);
    REG_BUILTIN("scalar", MIXED);
    REG_BUILTIN("keys", MIXED);
    REG_BUILTIN("values", MIXED);
    REG_BUILTIN("each", MIXED);

    /* ── Scalar::Util ───────────────────────────────────────────────
     * Source: RESEARCH.md L366. Exported subs; module QN "Scalar.Util". */
    REG_FUNC("Scalar.Util", "blessed", MIXED);
    REG_FUNC("Scalar.Util", "reftype", cbm_type_builtin(arena, "string"));
    REG_FUNC("Scalar.Util", "weaken", MIXED);

    /* ── List::Util ─────────────────────────────────────────────────
     * Source: RESEARCH.md L366. Module QN "List.Util". */
    REG_FUNC("List.Util", "sum", MIXED);
    REG_FUNC("List.Util", "max", MIXED);
    REG_FUNC("List.Util", "min", MIXED);
    REG_FUNC("List.Util", "first", MIXED);
    REG_FUNC("List.Util", "reduce", MIXED);

    /* ── Carp ───────────────────────────────────────────────────────
     * Source: RESEARCH.md L367. Module QN "Carp". */
    REG_FUNC("Carp", "croak", MIXED);
    REG_FUNC("Carp", "carp", MIXED);
    REG_FUNC("Carp", "confess", MIXED);
    REG_FUNC("Carp", "cluck", MIXED);

    /* ── POSIX (commonly-imported entry points) ─────────────────────
     * Source: RESEARCH.md L367. Module QN "POSIX". */
    REG_FUNC("POSIX", "floor", MIXED);
    REG_FUNC("POSIX", "ceil", MIXED);
    REG_FUNC("POSIX", "strftime", cbm_type_builtin(arena, "string"));
    REG_FUNC("POSIX", "INT_MAX", cbm_type_builtin(arena, "int"));

    /* ── Storable ───────────────────────────────────────────────────
     * Source: RESEARCH.md L367. Module QN "Storable". */
    REG_FUNC("Storable", "dclone", MIXED);
    REG_FUNC("Storable", "freeze", cbm_type_builtin(arena, "string"));
    REG_FUNC("Storable", "thaw", MIXED);
    REG_FUNC("Storable", "nstore", MIXED);
    REG_FUNC("Storable", "retrieve", MIXED);

    /* ── Data::Dumper ───────────────────────────────────────────────
     * Source: RESEARCH.md L367. Module QN "Data.Dumper". */
    REG_FUNC("Data.Dumper", "Dumper", cbm_type_builtin(arena, "string"));

    /* ═══ 5.38 expansion (perl-stdlib-538) ══════════════════════════
     * perlfunc built-ins beyond the seed set. Keep registry.c's
     * PERL_BUILTINS suppression list in lockstep — the two tables must
     * agree on what counts as a builtin (single-source comment there). */
    REG_BUILTIN("say", MIXED);
    REG_BUILTIN("lc", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("uc", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("lcfirst", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("ucfirst", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("index", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("rindex", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("abs", MIXED);
    REG_BUILTIN("int", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("hex", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("oct", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("ord", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("chr", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("sqrt", MIXED);
    REG_BUILTIN("rand", MIXED);
    REG_BUILTIN("srand", MIXED);
    REG_BUILTIN("pack", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("unpack", MIXED);
    REG_BUILTIN("reverse", MIXED);
    REG_BUILTIN("wantarray", MIXED);
    REG_BUILTIN("sleep", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("exit", MIXED);
    REG_BUILTIN("eval", MIXED);
    REG_BUILTIN("system", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("exec", MIXED);
    REG_BUILTIN("fork", MIXED);
    REG_BUILTIN("wait", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("waitpid", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("kill", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("localtime", MIXED);
    REG_BUILTIN("gmtime", MIXED);
    REG_BUILTIN("time", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("times", MIXED);
    REG_BUILTIN("mkdir", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("rmdir", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("opendir", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("readdir", MIXED);
    REG_BUILTIN("closedir", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("unlink", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("rename", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("stat", MIXED);
    REG_BUILTIN("lstat", MIXED);
    REG_BUILTIN("chdir", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("chmod", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("chown", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("symlink", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("readlink", MIXED);
    REG_BUILTIN("binmode", MIXED);
    REG_BUILTIN("read", MIXED);
    REG_BUILTIN("seek", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("tell", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("eof", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("fileno", cbm_type_builtin(arena, "int"));
    REG_BUILTIN("flock", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("truncate", cbm_type_builtin(arena, "bool"));
    REG_BUILTIN("select", MIXED);
    REG_BUILTIN("local", MIXED);
    REG_BUILTIN("tie", MIXED);
    REG_BUILTIN("untie", MIXED);
    REG_BUILTIN("tied", MIXED);
    REG_BUILTIN("caller", MIXED);
    REG_BUILTIN("sprintf", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("quotemeta", cbm_type_builtin(arena, "string"));
    REG_BUILTIN("study", MIXED);
    REG_BUILTIN("pos", MIXED);
    REG_BUILTIN("splice", MIXED);
    REG_BUILTIN("exists", cbm_type_builtin(arena, "bool"));

    /* ── List::Util (5.38 full common export set) ─────────────────── */
    REG_FUNC("List.Util", "sum0", MIXED);
    REG_FUNC("List.Util", "uniq", MIXED);
    REG_FUNC("List.Util", "uniqnum", MIXED);
    REG_FUNC("List.Util", "any", cbm_type_builtin(arena, "bool"));
    REG_FUNC("List.Util", "all", cbm_type_builtin(arena, "bool"));
    REG_FUNC("List.Util", "none", cbm_type_builtin(arena, "bool"));
    REG_FUNC("List.Util", "notall", cbm_type_builtin(arena, "bool"));
    REG_FUNC("List.Util", "maxstr", cbm_type_builtin(arena, "string"));
    REG_FUNC("List.Util", "minstr", cbm_type_builtin(arena, "string"));
    REG_FUNC("List.Util", "product", MIXED);
    REG_FUNC("List.Util", "shuffle", MIXED);
    REG_FUNC("List.Util", "head", MIXED);
    REG_FUNC("List.Util", "tail", MIXED);
    REG_FUNC("List.Util", "pairs", MIXED);
    REG_FUNC("List.Util", "pairkeys", MIXED);
    REG_FUNC("List.Util", "pairvalues", MIXED);
    REG_FUNC("List.Util", "pairmap", MIXED);
    REG_FUNC("List.Util", "pairgrep", MIXED);

    /* ── Scalar::Util (full common export set) ────────────────────── */
    REG_FUNC("Scalar.Util", "looks_like_number", cbm_type_builtin(arena, "bool"));
    REG_FUNC("Scalar.Util", "refaddr", cbm_type_builtin(arena, "int"));
    REG_FUNC("Scalar.Util", "dualvar", MIXED);
    REG_FUNC("Scalar.Util", "readonly", cbm_type_builtin(arena, "bool"));
    REG_FUNC("Scalar.Util", "isweak", cbm_type_builtin(arena, "bool"));
    REG_FUNC("Scalar.Util", "unweaken", MIXED);
    REG_FUNC("Scalar.Util", "openhandle", MIXED);
    REG_FUNC("Scalar.Util", "set_prototype", MIXED);

    /* ── File::Basename / File::Path / File::Copy / Cwd ───────────── */
    REG_FUNC("File.Basename", "basename", cbm_type_builtin(arena, "string"));
    REG_FUNC("File.Basename", "dirname", cbm_type_builtin(arena, "string"));
    REG_FUNC("File.Basename", "fileparse", MIXED);
    REG_FUNC("File.Path", "make_path", MIXED);
    REG_FUNC("File.Path", "remove_tree", MIXED);
    REG_FUNC("File.Path", "mkpath", MIXED);
    REG_FUNC("File.Path", "rmtree", MIXED);
    REG_FUNC("File.Copy", "copy", cbm_type_builtin(arena, "bool"));
    REG_FUNC("File.Copy", "move", cbm_type_builtin(arena, "bool"));
    REG_FUNC("File.Copy", "cp", cbm_type_builtin(arena, "bool"));
    REG_FUNC("File.Copy", "mv", cbm_type_builtin(arena, "bool"));
    REG_FUNC("Cwd", "getcwd", cbm_type_builtin(arena, "string"));
    REG_FUNC("Cwd", "cwd", cbm_type_builtin(arena, "string"));
    REG_FUNC("Cwd", "abs_path", cbm_type_builtin(arena, "string"));
    REG_FUNC("Cwd", "realpath", cbm_type_builtin(arena, "string"));

    /* ── Getopt::Long / Sys::Hostname / Pod::Usage ────────────────── */
    REG_FUNC("Getopt.Long", "GetOptions", cbm_type_builtin(arena, "bool"));
    REG_FUNC("Getopt.Long", "GetOptionsFromArray", cbm_type_builtin(arena, "bool"));
    REG_FUNC("Sys.Hostname", "hostname", cbm_type_builtin(arena, "string"));
    REG_FUNC("Pod.Usage", "pod2usage", MIXED);

    /* ── Time::HiRes ──────────────────────────────────────────────── */
    REG_FUNC("Time.HiRes", "time", MIXED);
    REG_FUNC("Time.HiRes", "sleep", MIXED);
    REG_FUNC("Time.HiRes", "usleep", MIXED);
    REG_FUNC("Time.HiRes", "nanosleep", MIXED);
    REG_FUNC("Time.HiRes", "gettimeofday", MIXED);
    REG_FUNC("Time.HiRes", "tv_interval", MIXED);

    /* ── Digest / MIME::Base64 / Encode / JSON::PP ────────────────── */
    REG_FUNC("Digest.MD5", "md5", cbm_type_builtin(arena, "string"));
    REG_FUNC("Digest.MD5", "md5_hex", cbm_type_builtin(arena, "string"));
    REG_FUNC("Digest.MD5", "md5_base64", cbm_type_builtin(arena, "string"));
    REG_FUNC("Digest.SHA", "sha1_hex", cbm_type_builtin(arena, "string"));
    REG_FUNC("Digest.SHA", "sha256_hex", cbm_type_builtin(arena, "string"));
    REG_FUNC("Digest.SHA", "sha512_hex", cbm_type_builtin(arena, "string"));
    REG_FUNC("MIME.Base64", "encode_base64", cbm_type_builtin(arena, "string"));
    REG_FUNC("MIME.Base64", "decode_base64", cbm_type_builtin(arena, "string"));
    REG_FUNC("Encode", "encode", cbm_type_builtin(arena, "string"));
    REG_FUNC("Encode", "decode", cbm_type_builtin(arena, "string"));
    REG_FUNC("Encode", "encode_utf8", cbm_type_builtin(arena, "string"));
    REG_FUNC("Encode", "decode_utf8", cbm_type_builtin(arena, "string"));
    REG_FUNC("JSON.PP", "encode_json", cbm_type_builtin(arena, "string"));
    REG_FUNC("JSON.PP", "decode_json", MIXED);

    /* ── POSIX (common subset; POSIX exports nearly everything by
     * default — model the high-frequency names, note the limitation) ─ */
    REG_FUNC("POSIX", "strtol", cbm_type_builtin(arena, "int"));
    REG_FUNC("POSIX", "strtod", MIXED);
    REG_FUNC("POSIX", "setlocale", cbm_type_builtin(arena, "string"));
    REG_FUNC("POSIX", "isatty", cbm_type_builtin(arena, "bool"));
    REG_FUNC("POSIX", "getpid", cbm_type_builtin(arena, "int"));
    REG_FUNC("POSIX", "dup2", cbm_type_builtin(arena, "int"));
    REG_FUNC("POSIX", "WIFEXITED", cbm_type_builtin(arena, "bool"));
    REG_FUNC("POSIX", "WEXITSTATUS", cbm_type_builtin(arena, "int"));
    REG_FUNC("POSIX", "SIGTERM", cbm_type_builtin(arena, "int"));
    REG_FUNC("POSIX", "fmod", MIXED);
    REG_FUNC("POSIX", "pow", MIXED);

    /* ── Socket / Term::ANSIColor ─────────────────────────────────── */
    REG_FUNC("Socket", "inet_aton", MIXED);
    REG_FUNC("Socket", "inet_ntoa", cbm_type_builtin(arena, "string"));
    REG_FUNC("Socket", "sockaddr_in", MIXED);
    REG_FUNC("Term.ANSIColor", "color", cbm_type_builtin(arena, "string"));
    REG_FUNC("Term.ANSIColor", "colored", cbm_type_builtin(arena, "string"));

    /* ── Test::More / Test2::V0 (perl-test-ecosystem dependency:
     * these make t/ files' assertion calls resolve) ────────────────── */
    REG_FUNC("Test.More", "ok", MIXED);
    REG_FUNC("Test.More", "is", MIXED);
    REG_FUNC("Test.More", "isnt", MIXED);
    REG_FUNC("Test.More", "like", MIXED);
    REG_FUNC("Test.More", "unlike", MIXED);
    REG_FUNC("Test.More", "cmp_ok", MIXED);
    REG_FUNC("Test.More", "is_deeply", MIXED);
    REG_FUNC("Test.More", "subtest", MIXED);
    REG_FUNC("Test.More", "plan", MIXED);
    REG_FUNC("Test.More", "done_testing", MIXED);
    REG_FUNC("Test.More", "diag", MIXED);
    REG_FUNC("Test.More", "note", MIXED);
    REG_FUNC("Test.More", "pass", MIXED);
    REG_FUNC("Test.More", "fail", MIXED);
    REG_FUNC("Test.More", "skip", MIXED);
    REG_FUNC("Test.More", "BAIL_OUT", MIXED);
    REG_FUNC("Test.More", "new_ok", MIXED);
    REG_FUNC("Test.More", "isa_ok", MIXED);
    REG_FUNC("Test.More", "can_ok", MIXED);
    REG_FUNC("Test2.V0", "ok", MIXED);
    REG_FUNC("Test2.V0", "is", MIXED);
    REG_FUNC("Test2.V0", "like", MIXED);
    REG_FUNC("Test2.V0", "subtest", MIXED);
    REG_FUNC("Test2.V0", "done_testing", MIXED);

    /* ── Curated OO types: typed chains for the dominant CPAN objects.
     * DBI->connect → $dbh(DBI.db) → prepare → $sth(DBI.st) → execute. */
    REG_TYPE("DBI");
    REG_TYPE("DBI.db");
    REG_TYPE("DBI.st");
    REG_METHOD("DBI", "connect", cbm_type_named(arena, "DBI.db"));
    REG_METHOD("DBI", "connect_cached", cbm_type_named(arena, "DBI.db"));
    REG_METHOD("DBI.db", "prepare", cbm_type_named(arena, "DBI.st"));
    REG_METHOD("DBI.db", "prepare_cached", cbm_type_named(arena, "DBI.st"));
    REG_METHOD("DBI.db", "do", MIXED);
    REG_METHOD("DBI.db", "selectall_arrayref", MIXED);
    REG_METHOD("DBI.db", "selectall_hashref", MIXED);
    REG_METHOD("DBI.db", "selectrow_array", MIXED);
    REG_METHOD("DBI.db", "selectrow_hashref", MIXED);
    REG_METHOD("DBI.db", "begin_work", MIXED);
    REG_METHOD("DBI.db", "commit", MIXED);
    REG_METHOD("DBI.db", "rollback", MIXED);
    REG_METHOD("DBI.db", "disconnect", MIXED);
    REG_METHOD("DBI.db", "quote", cbm_type_builtin(arena, "string"));
    REG_METHOD("DBI.db", "last_insert_id", cbm_type_builtin(arena, "int"));
    REG_METHOD("DBI.st", "execute", MIXED);
    REG_METHOD("DBI.st", "fetch", MIXED);
    REG_METHOD("DBI.st", "fetchrow_array", MIXED);
    REG_METHOD("DBI.st", "fetchrow_arrayref", MIXED);
    REG_METHOD("DBI.st", "fetchrow_hashref", MIXED);
    REG_METHOD("DBI.st", "fetchall_arrayref", MIXED);
    REG_METHOD("DBI.st", "fetchall_hashref", MIXED);
    REG_METHOD("DBI.st", "finish", MIXED);
    REG_METHOD("DBI.st", "rows", cbm_type_builtin(arena, "int"));
    REG_METHOD("DBI.st", "bind_param", MIXED);

    REG_TYPE("LWP.UserAgent");
    REG_TYPE("HTTP.Response");
    REG_METHOD("LWP.UserAgent", "new", cbm_type_named(arena, "LWP.UserAgent"));
    REG_METHOD("LWP.UserAgent", "get", cbm_type_named(arena, "HTTP.Response"));
    REG_METHOD("LWP.UserAgent", "post", cbm_type_named(arena, "HTTP.Response"));
    REG_METHOD("LWP.UserAgent", "head", cbm_type_named(arena, "HTTP.Response"));
    REG_METHOD("LWP.UserAgent", "put", cbm_type_named(arena, "HTTP.Response"));
    REG_METHOD("LWP.UserAgent", "delete", cbm_type_named(arena, "HTTP.Response"));
    REG_METHOD("LWP.UserAgent", "request", cbm_type_named(arena, "HTTP.Response"));
    REG_METHOD("HTTP.Response", "is_success", cbm_type_builtin(arena, "bool"));
    REG_METHOD("HTTP.Response", "code", cbm_type_builtin(arena, "int"));
    REG_METHOD("HTTP.Response", "content", cbm_type_builtin(arena, "string"));
    REG_METHOD("HTTP.Response", "decoded_content", cbm_type_builtin(arena, "string"));
    REG_METHOD("HTTP.Response", "status_line", cbm_type_builtin(arena, "string"));
    REG_METHOD("HTTP.Response", "header", cbm_type_builtin(arena, "string"));

    REG_TYPE("IO.File");
    REG_METHOD("IO.File", "new", cbm_type_named(arena, "IO.File"));
    REG_METHOD("IO.File", "open", cbm_type_builtin(arena, "bool"));
    REG_METHOD("IO.File", "close", cbm_type_builtin(arena, "bool"));
    REG_METHOD("IO.File", "getline", cbm_type_builtin(arena, "string"));
    REG_METHOD("IO.File", "getlines", MIXED);
    REG_METHOD("IO.File", "print", MIXED);
    REG_METHOD("IO.File", "eof", cbm_type_builtin(arena, "bool"));

    REG_TYPE("File.Temp");
    REG_METHOD("File.Temp", "new", cbm_type_named(arena, "File.Temp"));
    REG_METHOD("File.Temp", "filename", cbm_type_builtin(arena, "string"));
    REG_FUNC("File.Temp", "tempfile", MIXED);
    REG_FUNC("File.Temp", "tempdir", cbm_type_builtin(arena, "string"));

    REG_TYPE("Time.Piece");
    REG_METHOD("Time.Piece", "strftime", cbm_type_builtin(arena, "string"));
    REG_METHOD("Time.Piece", "epoch", cbm_type_builtin(arena, "int"));
    REG_METHOD("Time.Piece", "year", cbm_type_builtin(arena, "int"));
    REG_METHOD("Time.Piece", "mon", cbm_type_builtin(arena, "int"));
    REG_METHOD("Time.Piece", "mday", cbm_type_builtin(arena, "int"));
    REG_METHOD("Time.Piece", "datetime", cbm_type_builtin(arena, "string"));
    REG_METHOD("Time.Piece", "ymd", cbm_type_builtin(arena, "string"));

    REG_TYPE("File.Spec");
    REG_METHOD("File.Spec", "catfile", cbm_type_builtin(arena, "string"));
    REG_METHOD("File.Spec", "catdir", cbm_type_builtin(arena, "string"));
    REG_METHOD("File.Spec", "splitdir", MIXED);
    REG_METHOD("File.Spec", "rel2abs", cbm_type_builtin(arena, "string"));
    REG_METHOD("File.Spec", "abs2rel", cbm_type_builtin(arena, "string"));
    REG_METHOD("File.Spec", "tmpdir", cbm_type_builtin(arena, "string"));
}
