## Context

See proposal.md for motivation. Verified current state (against source):

- `cli_activation_production_reserve` (src/cli/cli.c:527) holds cohort EX locks + the startup lock, then calls `cbm_daemon_ipc_generation_probe_under_startup_lock` (cli.c:571-572). `generation != 0` ⇒ return BUSY (0) or error (cli.c:573-578).
- The probe's endpoint-probe branch (ipc.c) is deliberately fail-closed: a present-but-ECONNREFUSED socket counts as generation 1 (to protect saturated BSD listeners whose reserved backlog yields ECONNREFUSED while alive).
- All locks in the path are kernel-released on process death (flock LOCK_NB / fcntl F_SETLK / F_GETLK probe) — a dead process cannot keep a reservation. The only surviving state is the socket publication: `.sock` + `.anc` (+ identity commits), never unlinked by a kill -9.
- `cbm_daemon_ipc_stale_generation_cleanup` (ipc.c:2464, POSIX) already exists: it validates the startup lock matches, acquires a temporary lifetime reservation EX (refuses if a live daemon holds one), and unlinks only inode-matched provably-current stale identities. Used by daemon-start paths and `cbm_version_cohort_daemon_presence` (version_cohort.c:850-877).
- `cbm_daemon_ipc_lifetime_reservation_probe` (ipc.h:128): 0 = authoritatively absent, 1 = held, -1 = cannot validate.

## Goals / Non-Goals

**Goals:**
- A CLI activation reserve reaching `generation == 1` with a provably-absent lifetime reservation recovers: run the existing stale cleanup and re-probe before returning BUSY.
- Reuse the established repair; no new primitives (no new lock kinds, no new probe semantics).
- Live daemon safety by construction: cleanup itself re-checks + re-refuses; the CLI re-probe closes the window.

**Non-Goals:**
- Changing `cbm_daemon_ipc_endpoint_probe`'s ECONNREFUSED→active semantics (deliberate + tested; used by other security-sensitive callers).
- Windows behavior (rendezvous-record model; guarded out).
- Client-visible CLI surface changes (no flags, no new messages).
- Faster retry loop for the eager shutdown probe against stale sockets (a separate latency improvement, noted in the oracle report).

## Decisions

**D1 — Insertion point: `cli_activation_production_reserve`, right after the generation probe.**
Keeping it in the CLI layer (not in ipc.c) preserves the probe's semantics for other callers and keeps the recovery scoped to the activation decision. The startup lock is already held at this point (cli.c:566), which is exactly what the cleanup requires.

**D2 — Guarded by `#ifndef _WIN32` + three conditions.**
The recovery runs only when:
1. `generation == 1` (some publication claims an active generation), AND
2. `cbm_daemon_ipc_lifetime_reservation_probe(...) == 0` (no live daemon holds the lifetime reservation — every live listener does, from listen to close), AND
3. `cbm_daemon_ipc_stale_generation_cleanup(...) == 1` (cleanup itself validated + removed the stale identity; 0 = refused, -1 = invalid).

Then the generation probe runs once more (ENOENT ⇒ 0). Any other combination falls through to today's verdict.

**D3 — The re-probe is the anti-TOCTOU close.**
Only the re-probed 0 grants a clean path; if the re-probe still says 1, the original BUSY decision stands. No new code path can mutate anything: the existing `if (generation != 0)` branch is unchanged.

**D4 — Single guarded block, no new helper function.**
The block is 8 lines and needs the surrounding context (lease/lock lifecycle) — a helper would need the same arguments.

## Risks / Trade-offs

- [Recovery could race a daemon starting between probe and cleanup] → A boot must hold the startup lock; the CLI holds it throughout. Daemon already booted ⇒ lifetime reservation held ⇒ condition 2 excludes it. Double-sealed.
- [Saturated-listener regression] → That daemon holds its lifetime reservation ⇒ cleanup returns 0 ⇒ BUSY as today (test 1875 pattern).
- [Cleanup partially refuses (unknown identity preserved)] → returns 0 ⇒ conservative BUSY; the reporter keeps today's message. Acceptable; the reporter in #1760 has a *provably* stale pair.
- [Windows] → excluded by `#ifndef _WIN32`; no change.
- [Cost when clean] → one lifetime probe (F_GETLK) on the busy path where generation==1 only; no cost on the common clean path (generation==0 skips).

## Migration Plan

No data/config migration. Behavior change is additive recovery; rollback = revert.

## Open Questions

None.
