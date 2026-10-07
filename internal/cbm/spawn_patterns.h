/*
 * spawn_patterns.h — process-spawning call sites, per language.
 *
 * Like the HTTP client tables in service_patterns.h: a call that starts
 * another program (subprocess.run, exec.Command, posix_spawn, Process.Start,
 * Command::new, ...) becomes a SPAWNS edge to a Process node named after the
 * program it starts. Nothing resolves the program to code: the edge records
 * that the caller leaves the graph through a process boundary, and which
 * program it starts when the call spells it as a literal.
 */
#ifndef CBM_SPAWN_PATTERNS_H
#define CBM_SPAWN_PATTERNS_H

#include "cbm.h"

#include <stdbool.h>
#include <stddef.h>

/* How a table spelling matches a call. */
typedef enum {
    /* The full spelling, written out or reached through the file's imports
     * (`sp.run` with `import subprocess as sp`, `run` from
     * `from subprocess import run`, Go `exec.Command` from "os/exec"). */
    CBM_SPAWN_QUALIFIED = 0,
    /* A bare name (`system`, `popen`, `exec`): the spelling alone. A
     * definition of that name in the calling module (or one the call reaches
     * through an import) still makes it an ordinary call. */
    CBM_SPAWN_BARE,
    /* A bare name that counts when the file imports `module`
     * (Haskell `callProcess` with System.Process, a destructured
     * `const { spawn } = require("child_process")`). */
    CBM_SPAWN_BARE_IMPORTED,
    /* Groovy `"git status".execute()`: the receiver is the command line. */
    CBM_SPAWN_STRING_RECEIVER,
} cbm_spawn_rule_t;

typedef struct {
    const char *api;    /* canonical spelling: "subprocess.run", "os/exec.Command", "system" */
    const char *module; /* the import a CBM_SPAWN_BARE_IMPORTED name needs, else NULL */
    unsigned char rule; /* cbm_spawn_rule_t */
    signed char arg;    /* argument holding the program or command line */
    signed char alt;    /* second place to look, -1 for none (CreateProcess: name or line),
                         * or CBM_SPAWN_ANY_LITERAL */
} cbm_spawn_api_t;

/* `alt` value: the program is the first literal anywhere in the argument,
 * which is an options tuple or struct (Elixir {:spawn, "cat"}, Zig
 * .{ .argv = &.{"ls"} }). Elsewhere only a leading literal or a collection's
 * first element is the program. */
#define CBM_SPAWN_ANY_LITERAL (-2)

/* The spawn API a call names, or NULL. `imports` are the calling file's own
 * import statements (local name -> module path) as extracted, external
 * modules included: subprocess or os/exec never resolve to project nodes, so
 * the resolver's import map does not hold them. Whether the call is a spawn
 * after all is the resolver's to decide: an evidence-backed resolution onto
 * project code (an import, the same module, the LSP; never a name guess)
 * makes it an ordinary call (src/pipeline/pass_spawns.c). */
const cbm_spawn_api_t *cbm_spawn_match(CBMLanguage lang, const char *callee,
                                       const CBMImport *imports, int count);

/* The program a matched spawn starts: the first literal in its program
 * argument, that literal's first word, without directories
 * ("/usr/bin/git status" -> "git"). CBM_SPAWN_DYNAMIC when the program is
 * not a literal. Writes into buf (at least CBM_SPAWN_PROGRAM_MAX bytes). */
#define CBM_SPAWN_DYNAMIC "<dynamic>"
#define CBM_SPAWN_PROGRAM_MAX 64
void cbm_spawn_program(const cbm_spawn_api_t *api, CBMLanguage lang, const CBMCall *call, char *buf,
                       size_t n);

#endif
