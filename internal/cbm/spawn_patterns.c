/*
 * spawn_patterns.c — process-spawning call sites, per language (see the header).
 *
 * Spellings come from what the extractor records as the callee (checked per
 * language against real calls): Rust `Command::new`, Go `exec.Command`, Java
 * `Runtime.getRuntime().exec`, a constructor as its class name.
 */
#include "spawn_patterns.h"
#include "foundation/constants.h"

#include <stdio.h>
#include <string.h>

#define Q CBM_SPAWN_QUALIFIED
#define U CBM_SPAWN_BARE
#define I CBM_SPAWN_BARE_IMPORTED
#define S CBM_SPAWN_STRING_RECEIVER

/* libc / POSIX / Win32. posix_spawn and _spawn* take the path second;
 * CreateProcess names the program first or only in its command line. */
static const cbm_spawn_api_t c_family[] = {
    {"system", NULL, U, 0, -1},          {"std::system", NULL, U, 0, -1},
    {"_wsystem", NULL, U, 0, -1},        {"popen", NULL, U, 0, -1},
    {"_popen", NULL, U, 0, -1},          {"_wpopen", NULL, U, 0, -1},
    {"execl", NULL, U, 0, -1},           {"execle", NULL, U, 0, -1},
    {"execlp", NULL, U, 0, -1},          {"execv", NULL, U, 0, -1},
    {"execve", NULL, U, 0, -1},          {"execvp", NULL, U, 0, -1},
    {"execvpe", NULL, U, 0, -1},         {"fexecve", NULL, U, 0, -1},
    {"posix_spawn", NULL, U, 1, -1},     {"posix_spawnp", NULL, U, 1, -1},
    {"_execl", NULL, U, 0, -1},          {"_execv", NULL, U, 0, -1},
    {"_execlp", NULL, U, 0, -1},         {"_execvp", NULL, U, 0, -1},
    {"_spawnl", NULL, U, 1, -1},         {"_spawnle", NULL, U, 1, -1},
    {"_spawnlp", NULL, U, 1, -1},        {"_spawnv", NULL, U, 1, -1},
    {"_spawnve", NULL, U, 1, -1},        {"_spawnvp", NULL, U, 1, -1},
    {"_wspawnv", NULL, U, 1, -1},        {"_wspawnvp", NULL, U, 1, -1},
    {"CreateProcess", NULL, U, 0, 1},    {"CreateProcessA", NULL, U, 0, 1},
    {"CreateProcessW", NULL, U, 0, 1},   {"ShellExecute", NULL, U, 2, -1},
    {"ShellExecuteA", NULL, U, 2, -1},   {"ShellExecuteW", NULL, U, 2, -1},
    {"ShellExecuteExA", NULL, U, 0, -1}, {"ShellExecuteExW", NULL, U, 0, -1},
    {"WinExec", NULL, U, 0, -1},         {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t python[] = {
    {"subprocess.run", NULL, Q, 0, -1},
    {"subprocess.call", NULL, Q, 0, -1},
    {"subprocess.check_call", NULL, Q, 0, -1},
    {"subprocess.check_output", NULL, Q, 0, -1},
    {"subprocess.Popen", NULL, Q, 0, -1},
    {"subprocess.getoutput", NULL, Q, 0, -1},
    {"subprocess.getstatusoutput", NULL, Q, 0, -1},
    {"os.system", NULL, Q, 0, -1},
    {"os.popen", NULL, Q, 0, -1},
    {"os.startfile", NULL, Q, 0, -1},
    {"os.execl", NULL, Q, 0, -1},
    {"os.execle", NULL, Q, 0, -1},
    {"os.execlp", NULL, Q, 0, -1},
    {"os.execlpe", NULL, Q, 0, -1},
    {"os.execv", NULL, Q, 0, -1},
    {"os.execve", NULL, Q, 0, -1},
    {"os.execvp", NULL, Q, 0, -1},
    {"os.execvpe", NULL, Q, 0, -1},
    {"os.posix_spawn", NULL, Q, 0, -1},
    {"os.posix_spawnp", NULL, Q, 0, -1},
    {"os.spawnl", NULL, Q, 1, -1},
    {"os.spawnle", NULL, Q, 1, -1},
    {"os.spawnlp", NULL, Q, 1, -1},
    {"os.spawnlpe", NULL, Q, 1, -1},
    {"os.spawnv", NULL, Q, 1, -1},
    {"os.spawnve", NULL, Q, 1, -1},
    {"os.spawnvp", NULL, Q, 1, -1},
    {"os.spawnvpe", NULL, Q, 1, -1},
    {"asyncio.create_subprocess_exec", NULL, Q, 0, -1},
    {"asyncio.create_subprocess_shell", NULL, Q, 0, -1},
    {"pexpect.spawn", NULL, Q, 0, -1},
    {"pexpect.run", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

/* Node's child_process (also behind `node:`), the destructured-require form,
 * execa / cross-spawn, and the Bun and Deno globals. */
static const cbm_spawn_api_t js_family[] = {
    {"child_process.spawn", NULL, Q, 0, -1},
    {"child_process.spawnSync", NULL, Q, 0, -1},
    {"child_process.exec", NULL, Q, 0, -1},
    {"child_process.execSync", NULL, Q, 0, -1},
    {"child_process.execFile", NULL, Q, 0, -1},
    {"child_process.execFileSync", NULL, Q, 0, -1},
    {"child_process.fork", NULL, Q, 0, -1},
    {"spawn", "child_process", I, 0, -1},
    {"spawnSync", "child_process", I, 0, -1},
    {"exec", "child_process", I, 0, -1},
    {"execSync", "child_process", I, 0, -1},
    {"execFile", "child_process", I, 0, -1},
    {"execFileSync", "child_process", I, 0, -1},
    {"fork", "child_process", I, 0, -1},
    {"execa.execa", NULL, Q, 0, -1},
    {"execa.execaSync", NULL, Q, 0, -1},
    {"execa.execaCommand", NULL, Q, 0, -1},
    {"execa.execaCommandSync", NULL, Q, 0, -1},
    {"cross-spawn.spawn", NULL, Q, 0, -1},
    {"cross-spawn.sync", NULL, Q, 0, -1},
    {"Bun.spawn", NULL, Q, 0, -1},
    {"Bun.spawnSync", NULL, Q, 0, -1},
    {"Deno.Command", NULL, Q, 0, -1},
    {"Deno.run", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t go[] = {
    {"os/exec.Command", NULL, Q, 0, -1},
    {"os/exec.CommandContext", NULL, Q, 1, -1},
    {"golang.org/x/sys/execabs.Command", NULL, Q, 0, -1},
    {"golang.org/x/sys/execabs.CommandContext", NULL, Q, 1, -1},
    {"os.StartProcess", NULL, Q, 0, -1},
    {"syscall.Exec", NULL, Q, 0, -1},
    {"syscall.ForkExec", NULL, Q, 0, -1},
    {"syscall.StartProcess", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t rust[] = {
    {"std::process::Command::new", NULL, Q, 0, -1},
    {"tokio::process::Command::new", NULL, Q, 0, -1},
    {"async_process::Command::new", NULL, Q, 0, -1},
    {"async_std::process::Command::new", NULL, Q, 0, -1},
    {"smol::process::Command::new", NULL, Q, 0, -1},
    {"duct::cmd", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

/* java.lang is implicit, so the class name alone is the spelling. */
static const cbm_spawn_api_t jvm[] = {
    {"ProcessBuilder", NULL, U, 0, -1},
    {"java.lang.ProcessBuilder", NULL, Q, 0, -1},
    {"Runtime.getRuntime().exec", NULL, U, 0, -1},
    {".execute", NULL, S, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t dotnet[] = {
    {"Process.Start", NULL, U, 0, -1},    {"System.Diagnostics.Process.Start", NULL, Q, 0, -1},
    {"ProcessStartInfo", NULL, U, 0, -1}, {"System.Diagnostics.ProcessStartInfo", NULL, Q, 0, -1},
    {"Cli.Wrap", NULL, U, 0, -1},         {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t ruby[] = {
    {"system", NULL, U, 0, -1},          {"exec", NULL, U, 0, -1},
    {"spawn", NULL, U, 0, -1},           {"Kernel.system", NULL, U, 0, -1},
    {"Kernel.exec", NULL, U, 0, -1},     {"Kernel.spawn", NULL, U, 0, -1},
    {"Process.spawn", NULL, U, 0, -1},   {"Process.exec", NULL, U, 0, -1},
    {"IO.popen", NULL, U, 0, -1},        {"PTY.spawn", NULL, U, 0, -1},
    {"Open3.popen2", NULL, U, 0, -1},    {"Open3.popen2e", NULL, U, 0, -1},
    {"Open3.popen3", NULL, U, 0, -1},    {"Open3.capture2", NULL, U, 0, -1},
    {"Open3.capture2e", NULL, U, 0, -1}, {"Open3.capture3", NULL, U, 0, -1},
    {"Open3.pipeline", NULL, U, 0, -1},  {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t php[] = {
    {"exec", NULL, U, 0, -1},
    {"shell_exec", NULL, U, 0, -1},
    {"system", NULL, U, 0, -1},
    {"passthru", NULL, U, 0, -1},
    {"proc_open", NULL, U, 0, -1},
    {"popen", NULL, U, 0, -1},
    {"pcntl_exec", NULL, U, 0, -1},
    {"Symfony\\Component\\Process\\Process", NULL, Q, 0, -1},
    {"Symfony\\Component\\Process\\Process::fromShellCommandline", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t perl[] = {
    {"system", NULL, U, 0, -1},
    {"exec", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t lua[] = {
    {"os.execute", NULL, U, 0, -1},
    {"io.popen", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t r_lang[] = {
    {"system", NULL, U, 0, -1},
    {"system2", NULL, U, 0, -1},
    {"shell", NULL, U, 0, -1},
    {"processx::run", NULL, Q, 0, -1},
    {"sys::exec_wait", NULL, Q, 0, -1},
    {"sys::exec_internal", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t elixir[] = {
    {"System.cmd", NULL, U, 0, -1},
    {"System.shell", NULL, U, 0, -1},
    {"Port.open", NULL, U, 0, CBM_SPAWN_ANY_LITERAL},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t erlang[] = {
    {"os:cmd", NULL, Q, 0, -1},
    {"open_port", NULL, U, 0, -1},
    {"erlang:open_port", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t haskell[] = {
    {"callProcess", "System.Process", I, 0, -1},
    {"callCommand", "System.Process", I, 0, -1},
    {"readProcess", "System.Process", I, 0, -1},
    {"readProcessWithExitCode", "System.Process", I, 0, -1},
    {"readCreateProcess", "System.Process", I, 0, -1},
    {"readCreateProcessWithExitCode", "System.Process", I, 0, -1},
    {"createProcess", "System.Process", I, 0, -1},
    {"spawnProcess", "System.Process", I, 0, -1},
    {"spawnCommand", "System.Process", I, 0, -1},
    {"system", "System.Process", I, 0, -1},
    {"rawSystem", "System.Process", I, 0, -1},
    {"runProcess", "System.Process", I, 0, -1},
    {"runCommand", "System.Process", I, 0, -1},
    {"runInteractiveProcess", "System.Process", I, 0, -1},
    {"runInteractiveCommand", "System.Process", I, 0, -1},
    {"runProcess", "System.Process.Typed", I, 0, -1},
    {"readProcess", "System.Process.Typed", I, 0, -1},
    {"startProcess", "System.Process.Typed", I, 0, -1},
    {"system", "System.Cmd", I, 0, -1},
    {"rawSystem", "System.Cmd", I, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t dlang[] = {
    {"execute", "std.process", I, 0, -1},
    {"executeShell", "std.process", I, 0, -1},
    {"spawnProcess", "std.process", I, 0, -1},
    {"spawnShell", "std.process", I, 0, -1},
    {"pipeProcess", "std.process", I, 0, -1},
    {"pipeShell", "std.process", I, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

/* Foundation's Process: the program is a property set after construction,
 * so a bare construction is a spawn site with a dynamic program. */
static const cbm_spawn_api_t swift[] = {
    {"Process.launchedProcess", NULL, U, 0, -1},
    {"Process.run", NULL, U, 0, -1},
    {"Process", "Foundation", I, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t crystal[] = {
    {"Process.run", NULL, U, 0, -1},  {"Process.new", NULL, U, 0, -1},
    {"Process.exec", NULL, U, 0, -1}, {"system", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t zig[] = {
    {"std.process.Child.init", NULL, Q, 0, -1},
    {"std.process.Child.run", NULL, Q, 0, CBM_SPAWN_ANY_LITERAL},
    {"std.ChildProcess.init", NULL, Q, 0, -1},
    {"std.ChildProcess.run", NULL, Q, 0, CBM_SPAWN_ANY_LITERAL},
    {"std.ChildProcess.exec", NULL, Q, 0, CBM_SPAWN_ANY_LITERAL},
    {"std.process.execv", NULL, Q, 1, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t ocaml[] = {
    {"Sys.command", NULL, U, 0, -1},
    {"Unix.system", NULL, U, 0, -1},
    {"Unix.create_process", NULL, U, 0, -1},
    {"Unix.create_process_env", NULL, U, 0, -1},
    {"Unix.open_process", NULL, U, 0, -1},
    {"Unix.open_process_in", NULL, U, 0, -1},
    {"Unix.open_process_out", NULL, U, 0, -1},
    {"Unix.open_process_full", NULL, U, 0, -1},
    {"Unix.open_process_args", NULL, U, 0, -1},
    {"Unix.execv", NULL, U, 0, -1},
    {"Unix.execvp", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t clojure[] = {
    {"clojure.java.shell/sh", NULL, Q, 0, -1},
    {"sh", "clojure.java.shell", I, 0, -1},
    {"babashka.process/process", NULL, Q, 0, -1},
    {"babashka.process/shell", NULL, Q, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t tcl[] = {
    {"exec", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t fortran[] = {
    {"execute_command_line", NULL, U, 0, -1},
    {"system", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t julia[] = {
    {"run", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

static const cbm_spawn_api_t powershell[] = {
    {"Start-Process", NULL, U, 0, -1},
    {NULL, NULL, 0, 0, -1},
};

#undef Q
#undef U
#undef I
#undef S

static const cbm_spawn_api_t *spawn_table(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_OBJC:
        return c_family;
    case CBM_LANG_PYTHON:
        return python;
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_ARKTS:
        return js_family;
    case CBM_LANG_GO:
        return go;
    case CBM_LANG_RUST:
        return rust;
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
    case CBM_LANG_GROOVY:
        return jvm;
    case CBM_LANG_CSHARP:
    case CBM_LANG_FSHARP:
        return dotnet;
    case CBM_LANG_RUBY:
        return ruby;
    case CBM_LANG_PHP:
        return php;
    case CBM_LANG_PERL:
        return perl;
    case CBM_LANG_LUA:
    case CBM_LANG_LUAU:
    case CBM_LANG_TEAL:
        return lua;
    case CBM_LANG_R:
        return r_lang;
    case CBM_LANG_ELIXIR:
        return elixir;
    case CBM_LANG_ERLANG:
        return erlang;
    case CBM_LANG_HASKELL:
        return haskell;
    case CBM_LANG_DLANG:
        return dlang;
    case CBM_LANG_SWIFT:
        return swift;
    case CBM_LANG_CRYSTAL:
        return crystal;
    case CBM_LANG_ZIG:
        return zig;
    case CBM_LANG_OCAML:
        return ocaml;
    case CBM_LANG_CLOJURE:
        return clojure;
    case CBM_LANG_TCL:
        return tcl;
    case CBM_LANG_FORTRAN:
        return fortran;
    case CBM_LANG_JULIA:
        return julia;
    case CBM_LANG_POWERSHELL:
        return powershell;
    default:
        return NULL;
    }
}

/* An import value without Node's `node:` scheme. */
static const char spawn_node_scheme[] = "node:";
static const char *spawn_module(const char *val) {
    size_t n = sizeof(spawn_node_scheme) - 1;
    return strncmp(val, spawn_node_scheme, n) == 0 ? val + n : val;
}

static const char *const spawn_separators[] = {"::", ".", ":", "/", "\\", NULL};

/* Length of the callee's head (before its first separator) and the
 * separator's offset, or 0 when the callee has none. */
static size_t spawn_head_len(const char *callee) {
    size_t best = 0;
    for (int i = 0; spawn_separators[i]; i++) {
        const char *at = strstr(callee, spawn_separators[i]);
        if (at && at > callee && (best == 0 || (size_t)(at - callee) < best)) {
            best = (size_t)(at - callee);
        }
    }
    return best;
}

/* True when `val` names `name` as its last segment ("subprocess.run" for
 * run, "java.lang.ProcessBuilder" for ProcessBuilder). */
static bool spawn_val_names(const char *val, const char *name) {
    size_t vl = strlen(val);
    size_t nl = strlen(name);
    if (vl <= nl || strcmp(val + vl - nl, name) != 0) {
        return false;
    }
    char sep = val[vl - nl - 1];
    return sep == '.' || sep == ':' || sep == '/' || sep == '\\';
}

/* The callee as its import makes it: `sp.run` -> subprocess.run (an alias),
 * `run` -> subprocess.run (a member import), `spawn` -> child_process.spawn
 * (a named import mapped to its module). Empty when no import applies. */
static void spawn_expand(const char *callee, const CBMImport *imports, int count, char *out,
                         size_t n) {
    out[0] = '\0';
    size_t head = spawn_head_len(callee);
    size_t key_len = head ? head : strlen(callee);
    for (int i = 0; i < count; i++) {
        const char *key = imports[i].local_name;
        if (!key || !imports[i].module_path || strlen(key) != key_len ||
            strncmp(key, callee, key_len) != 0) {
            continue;
        }
        const char *val = spawn_module(imports[i].module_path);
        if (head) {
            snprintf(out, n, "%s%s", val, callee + head);
        } else if (spawn_val_names(val, callee)) {
            snprintf(out, n, "%s", val);
        } else {
            snprintf(out, n, "%s.%s", val, callee);
        }
        return;
    }
}

static bool spawn_imports_module(const CBMImport *imports, int count, const char *module) {
    size_t ml = strlen(module);
    for (int i = 0; i < count; i++) {
        if (!imports[i].module_path) {
            continue;
        }
        const char *val = spawn_module(imports[i].module_path);
        if (strncmp(val, module, ml) == 0 &&
            (val[ml] == '\0' || val[ml] == '.' || val[ml] == ':' || val[ml] == '/')) {
            return true;
        }
    }
    return false;
}

/* Groovy `"git status".execute`: a quoted receiver, then the api suffix. */
static bool spawn_string_receiver(const char *callee, const char *suffix) {
    size_t cl = strlen(callee);
    size_t sl = strlen(suffix);
    return (callee[0] == '"' || callee[0] == '\'') && cl > sl + 1 &&
           strcmp(callee + cl - sl, suffix) == 0 && callee[cl - sl - 1] == callee[0];
}

static bool spawn_entry_matches(const cbm_spawn_api_t *e, const char *callee, const char *expanded,
                                const CBMImport *imports, int count) {
    switch (e->rule) {
    case CBM_SPAWN_QUALIFIED:
        return strcmp(callee, e->api) == 0 || (expanded[0] && strcmp(expanded, e->api) == 0);
    case CBM_SPAWN_BARE:
        return strcmp(callee, e->api) == 0;
    case CBM_SPAWN_BARE_IMPORTED:
        return strcmp(callee, e->api) == 0 && spawn_imports_module(imports, count, e->module);
    case CBM_SPAWN_STRING_RECEIVER:
        return spawn_string_receiver(callee, e->api);
    default:
        return false;
    }
}

const cbm_spawn_api_t *cbm_spawn_match(CBMLanguage lang, const char *callee,
                                       const CBMImport *imports, int count) {
    const cbm_spawn_api_t *table = spawn_table(lang);
    if (!table || !callee || !callee[0]) {
        return NULL;
    }
    if (!imports) {
        count = 0;
    }
    char expanded[CBM_SZ_512];
    spawn_expand(callee, imports, count, expanded, sizeof(expanded));
    for (const cbm_spawn_api_t *e = table; e->api; e++) {
        if (spawn_entry_matches(e, callee, expanded, imports, count)) {
            return e;
        }
    }
    return NULL;
}

/* The first quoted literal in `text` ("..", '..' or `..`), copied without
 * its quotes. False when there is none. */
static bool spawn_first_literal(const char *text, char *out, size_t n) {
    for (const char *p = text; *p; p++) {
        if (*p != '"' && *p != '\'' && *p != '`') {
            continue;
        }
        const char *end = p + 1;
        while (*end && *end != *p) {
            end += (*end == '\\' && end[1]) ? 2 : 1;
        }
        if (!*end) {
            return false;
        }
        size_t len = (size_t)(end - p - 1);
        if (len >= n) {
            len = n - 1;
        }
        memcpy(out, p + 1, len);
        out[len] = '\0';
        return true;
    }
    return false;
}

static const char *spawn_arg_expr(const CBMCall *call, int index) {
    for (int i = 0; i < call->arg_count; i++) {
        if (call->args[i].index == index && call->args[i].expr) {
            return call->args[i].expr;
        }
    }
    return NULL;
}

static bool spawn_word_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

static const char *spawn_skip_space(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
        p++;
    }
    return p;
}

/* Past a keyword label in front of an argument (`launchPath: `, `args=`). */
static const char *spawn_skip_label(const char *p) {
    const char *q = p;
    while (spawn_word_char(*q)) {
        q++;
    }
    if (q == p) {
        return p;
    }
    q = spawn_skip_space(q);
    if ((*q == ':' && q[1] != ':') || (*q == '=' && q[1] != '=')) {
        return spawn_skip_space(q + 1);
    }
    return p;
}

/* A string literal at the start of `p`, after a prefix such as r, b, f, L
 * or @ (r"..", L"..", @".."). */
static bool spawn_leading_literal(const char *p, char *out, size_t n) {
    for (int i = 0; i < 2 && *p && strchr("rRbBuUfFL@$", *p); i++) {
        p++;
    }
    return (*p == '"' || *p == '\'' || *p == '`') && spawn_first_literal(p, out, n);
}

/* What may stand before a collection's bracket: nothing, Zig's `&.` / `.`,
 * Java's `new String[]`, or a collection constructor. A name before `[` is a
 * subscript (`record["argv"]`) and any other call's arguments are not the
 * command (`os.path.join("bin", tool)`). */
static bool spawn_collection_prefix(const char *p, size_t len) {
    static const char *const ctors[] = {
        "listOf", "arrayOf", "mutableListOf", "List.of", "Arrays.asList", "vec!", "c", "Cmd", NULL};
    if (len == 0 || (len <= 2 && strspn(p, "&.") == len) ||
        (len > 2 && strncmp(p + len - 2, "[]", 2) == 0 && strncmp(p, "new ", 4) == 0)) {
        return true;
    }
    for (int i = 0; ctors[i]; i++) {
        if (strlen(ctors[i]) == len && strncmp(p, ctors[i], len) == 0) {
            return true;
        }
    }
    return false;
}

/* The first element of a collection written at `p` ([..], (..), {..},
 * listOf(..), &.{..}), copied into `out`. False when `p` is no collection. */
static bool spawn_first_element(const char *p, char *out, size_t n) {
    const char *open = p;
    while (*open && (spawn_word_char(*open) || strchr("!.&:<> ", *open) ||
                     (open[0] == '[' && open[1] == ']'))) {
        open += (open[0] == '[') ? 2 : 1;
    }
    if ((*open != '[' && *open != '(' && *open != '{') ||
        !spawn_collection_prefix(p, (size_t)(open - p))) {
        return false;
    }
    p = open;
    const char *start = spawn_skip_space(p + 1);
    int depth = 0;
    const char *end = start;
    for (; *end; end++) {
        if (*end == '"' || *end == '\'' || *end == '`') {
            const char *close = strchr(end + 1, *end);
            if (!close) {
                break;
            }
            end = close;
        } else if (strchr("[({", *end)) {
            depth++;
        } else if (strchr("])}", *end)) {
            if (depth-- == 0) {
                break;
            }
        } else if (*end == ',' && depth == 0) {
            break;
        }
    }
    size_t len = (size_t)(end - start);
    if (len == 0 || len >= n) {
        return false;
    }
    memcpy(out, start, len);
    out[len] = '\0';
    return true;
}

/* The program literal an argument spells: a leading literal, or the first
 * element of a collection when that element is one. A program held in a
 * variable (`[binary, "--version"]`) is no literal: the arguments after it
 * are not the program. */
static bool spawn_expr_literal(const char *expr, char *out, size_t n) {
    char element[CBM_SZ_256];
    for (int depth = 0; depth < 3; depth++) {
        expr = spawn_skip_label(spawn_skip_space(expr));
        if (spawn_leading_literal(expr, out, n)) {
            return true;
        }
        if (!spawn_first_element(expr, element, sizeof(element))) {
            return false;
        }
        expr = element;
    }
    return false;
}

/* A literal from the argument at `index`. Tcl words are literals unquoted;
 * elsewhere an unquoted argument counts only when the extractor itself read
 * it as the call's string (Perl's `system("make")`). */
static bool spawn_arg_literal(const CBMCall *call, CBMLanguage lang, int index, bool anywhere,
                              char *out, size_t n) {
    const char *expr = spawn_arg_expr(call, index);
    if (!expr) {
        if (call->arg_count == 0 && index == 0 && call->first_string_arg) {
            snprintf(out, n, "%s", call->first_string_arg);
            return true;
        }
        return false;
    }
    if (anywhere ? spawn_first_literal(expr, out, n) : spawn_expr_literal(expr, out, n)) {
        return true;
    }
    bool bareword = lang == CBM_LANG_TCL && strpbrk(expr, "$[") == NULL;
    if (bareword || (call->first_string_arg && strcmp(expr, call->first_string_arg) == 0)) {
        snprintf(out, n, "%s", expr);
        return true;
    }
    return false;
}

/* A shell word that is not the program: an environment assignment
 * (`LC_ALL=C make`) or the `env` that runs the program after them
 * (`/usr/bin/env make`). */
static bool spawn_prefix_word(const char *word, size_t len) {
    static const char env[] = "env";
    size_t env_len = sizeof(env) - 1;
    if (len >= env_len && strncmp(word + len - env_len, env, env_len) == 0 &&
        (len == env_len || word[len - env_len - 1] == '/')) {
        return true;
    }
    size_t i = 0;
    while (i < len && spawn_word_char(word[i])) {
        i++;
    }
    return i > 0 && i < len && word[i] == '=';
}

/* The program word of a command line: its first word that is not an
 * environment prefix, without directories. Anything built at run time is not
 * a program name. */
static void spawn_program_word(const char *literal, char *buf, size_t n) {
    size_t len = 0;
    for (;;) {
        while (*literal == ' ' || *literal == '\t') {
            literal++;
        }
        len = strcspn(literal, " \t\r\n");
        if (!spawn_prefix_word(literal, len) || !literal[len]) {
            break;
        }
        literal += len;
    }
    const char *start = literal;
    for (size_t i = 0; i < len; i++) {
        if (literal[i] == '/' || literal[i] == '\\') {
            start = literal + i + 1;
        }
    }
    len -= (size_t)(start - literal);
    bool dynamic = len == 0 || len >= CBM_SPAWN_PROGRAM_MAX || len >= n;
    for (size_t i = 0; i < len && !dynamic; i++) {
        dynamic = strchr("$%{}`()<>|;&*?[]\"'", start[i]) != NULL;
    }
    if (dynamic) {
        snprintf(buf, n, "%s", CBM_SPAWN_DYNAMIC);
        return;
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
}

void cbm_spawn_program(const cbm_spawn_api_t *api, CBMLanguage lang, const CBMCall *call, char *buf,
                       size_t n) {
    char literal[CBM_SZ_256];
    bool found = false;
    if (api->rule == CBM_SPAWN_STRING_RECEIVER) {
        found = spawn_first_literal(call->callee_name, literal, sizeof(literal));
    } else {
        bool anywhere = api->alt == CBM_SPAWN_ANY_LITERAL;
        found = spawn_arg_literal(call, lang, api->arg, anywhere, literal, sizeof(literal)) ||
                (api->alt >= 0 &&
                 spawn_arg_literal(call, lang, api->alt, false, literal, sizeof(literal)));
    }
    if (!found) {
        snprintf(buf, n, "%s", CBM_SPAWN_DYNAMIC);
        return;
    }
    spawn_program_word(literal, buf, n);
}
