/*
 * index_supervisor.h — run index_repository in a supervised worker subprocess.
 *
 * A single pathological file can hard-crash (SIGSEGV / stack overflow / abort) or
 * hang the native indexer, and today that takes down the whole MCP server or CLI.
 * The supervisor runs the actual index in a CHILD process (the same binary
 * re-invoked as `cli --index-worker index_repository …`), reaps it, and classifies
 * how it ended. A crash/hang is contained to the child; the parent survives and
 * reports it instead of dying.
 *
 * This module owns only the spawn/reap MECHANISM and the worker-role state. The
 * MCP handler (mcp.c) owns the gate placement and the response building, so this
 * module has no dependency on the response format.
 *
 * fork+exec only (never fork-and-run-in-child): the server holds persistent
 * threads plus mimalloc/sqlite global state with no pthread_atfork, so a
 * fork without exec would be a latent deadlock. Recursion is prevented by an argv
 * flag (`--index-worker`), never an ambient env var.
 */
#ifndef CBM_INDEX_SUPERVISOR_H
#define CBM_INDEX_SUPERVISOR_H

#include <stdbool.h>

#include "foundation/subprocess.h" /* cbm_proc_outcome_t */

/* Worker-role state, set once from the CLI arg parser (main.c) when this process
 * was spawned as a supervised worker. When active, indexing must run in-process
 * (the gate must NOT re-supervise). response_out (may be NULL) is the file the
 * worker writes its final result string to, for the parent to read back. */
void cbm_index_set_worker_role(bool is_worker, const char *response_out);
bool cbm_index_worker_active(void);
const char *cbm_index_worker_response_out(void);

/* Host marking (#845): the supervisor gate is OPT-IN per process. Only the real
 * codebase-memory-mcp binary calls this (first thing in main(), before any
 * subcommand dispatch, so MCP server + CLI + HTTP paths are all covered).
 * EMBEDDERS of cbm_mcp_handle_tool (test binaries, future library users) never
 * call it, so they index in-process by default. Without this gate the supervisor
 * resolved the CURRENT executable and re-invoked it as
 * `<self> cli --index-worker …` — a test binary ignores those args and re-runs
 * its suites instead, producing recursive spawn chains (11-min hangs; kernel
 * VM-map pressure during the 2026-07-04 host panics). */
void cbm_index_supervisor_mark_host(void);

/* True when handle_index_repository should wrap the run in a supervised child:
 * this process called cbm_index_supervisor_mark_host() (i.e. it IS the real
 * binary, not an embedder), is not itself a worker, AND the kill switch
 * (CBM_INDEX_SUPERVISOR=0) is not set. */
bool cbm_index_supervisor_should_wrap(void);

/* TEST HOOK (#845): process-wide count of worker-spawn attempts, incremented on
 * entry to cbm_index_spawn_worker. Embedder tests assert the count is unchanged
 * across an index_repository call to prove indexing ran IN-PROCESS. */
int cbm_index_supervisor_spawn_count(void);

/* Test hook: single-threaded spawn count — must stay ZERO (production
 * recovery is parallel-only; no sequential runs). */
int cbm_index_supervisor_spawn_st_count(void);

typedef struct {
    cbm_proc_outcome_t outcome; /* how the worker ended */
    int exit_code;              /* worker exit code (-1 if signalled) */
    int term_signal;            /* POSIX terminating signal, else 0 */
    char *response;             /* worker's result string on CLEAN exit (caller frees); else NULL */
} cbm_index_worker_result_t;

/* Spawn `<self> cli --index-worker index_repository <args_json> --response-out <tmp>`,
 * supervise it (quiet-timeout for hangs), reap, and classify. On a clean exit,
 * result->response holds the worker's response string (read from the temp file).
 * Returns 0 if a worker was spawned and reaped (result filled), or -1 if the
 * child could not be spawned (caller degrades to in-process).
 *
 * Probe knobs for the skip-and-continue recovery re-run (Stage 3c) are passed to
 * the child as inherited env vars around the spawn (set before, unset after —
 * safe because spawns are sequential):
 *   - single_thread   → CBM_INDEX_SINGLE_THREAD=1: the pipeline uses exactly one
 *                       worker, so a per-file marker pins the EXACT crasher.
 *   - marker_file      → CBM_INDEX_MARKER_FILE: the worker writes the rel_path of
 *                       the file it is about to process here before touching it.
 *   - quarantine_file  → CBM_INDEX_QUARANTINE_FILE: newline-delimited rel_paths to
 *                       skip and report as phase="crash".
 * Any of the three may be false/NULL to leave that knob unset (the normal first
 * attempt passes single_thread=false, marker_file=NULL, quarantine_file=NULL). */
int cbm_index_spawn_worker(const char *args_json, bool single_thread, const char *marker_file,
                           const char *quarantine_file, cbm_index_worker_result_t *result);

void cbm_index_worker_result_free(cbm_index_worker_result_t *result);

#endif /* CBM_INDEX_SUPERVISOR_H */
