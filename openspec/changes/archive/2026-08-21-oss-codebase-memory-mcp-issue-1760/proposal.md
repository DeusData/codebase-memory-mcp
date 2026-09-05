## Why

After CBM daemon processes are hard-killed (`pkill`/`kill -9`), the rendezvous directory under the runtime dir keeps the published Unix socket identity (`.sock`/`.anc` pair, no `.lock`/`.pid`). Every subsequent `install`/`update` then fails permanently with "active CBM sessions ... could not be stopped safely" even though no process, lock, or socket endpoint is alive. The socket publication survives the hard kill (it is an artifact of bind/linkat, not a held file lock), and the activation guard's generation probe treats an `ECONNREFUSED` socket as an active generation (fail-closed, deliberate for saturated BSD listeners). The stale socket is only repaired on daemon start paths — which the CLI never reaches because it refuses before mutating anything. Deadlock by residue.

## What Changes

- During a CLI activation reserve (`cli_activation_production_reserve`, the path behind `install`/`update`), when the generation probe reports an active generation (1) **but** the lifetime reservation probe reports none (0) and the startup lock is held (it is), the stale socket identity is provably residual publication of a dead process. The CLI then runs the existing `cbm_daemon_ipc_stale_generation_cleanup` repair (same one daemon-start paths and `cbm_version_cohort_daemon_presence` use) and re-probes generation before deciding.
- `stale_generation_cleanup` refuses (returns 0) when a lifetime reservation is genuinely held or the identity is mismatched/unknown — so a live daemon (including a saturated BSD listener) is never disturbed; the guard keeps today's BUSY verdict in every such case.
- POSIX-only. Windows keeps its existing rendezvous-record based semantics unchanged.
- No public CLI/API shape changes: same commands, same ordering, same messages — a previously impossible recovery path only.

## Capabilities

### New Capabilities
- `cli-activation-stale-rendezvous-recovery`: CLI activation's recovery of residual rendezvous state left by hard-killed daemon processes before declaring BUSY.

### Modified Capabilities
- None (no existing specs).

## Impact

- `src/cli/cli.c` — `cli_activation_production_reserve`: probe → (generation 1 + lifetime 0) → cleanup → re-probe before returning BUSY.
- `tests/test_cli.c` and/or `tests/test_daemon_ipc.c` — regression tests: stale socket produced by bind+listen+close (no unlink) under an isolated runtime parent is recovered by the activation guard; a LIVE listener (kept open) still blocks (BUSY).
- Reuses existing `cbm_daemon_ipc_stale_generation_cleanup`; no new lock, no new IPC surface.
