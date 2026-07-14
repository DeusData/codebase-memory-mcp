#!/usr/bin/env bash
# test_worker_watchdog.sh — regression guard for the WORKER-mode parent-death
# watchdog (#845). A supervised index worker (`cli --index-worker
# index_repository …`) whose supervisor dies must exit on its own instead of
# indexing on as an orphan (orphaned workers contributed to memory pressure
# during the 2026-07-04 host panics). The MCP-server watchdog (#406/#407,
# tests/test_parent_watchdog.sh) did not cover CLI worker mode.
#
# Strategy: launch the worker under a wrapper "parent" on a fixture where the
# test-only injector (CBM_TEST_HANG_ON) busy-spins on one file, so the worker
# is guaranteed to still be mid-index when the wrapper is killed — the guard
# cannot pass vacuously via the worker simply finishing. CBM_INDEX_SINGLE_THREAD
# + CBM_INDEX_MARKER_FILE (the supervisor's own recovery knobs) give a
# deterministic "worker is AT the hang file" sync point: the worker writes the
# rel_path it is about to process before touching it. After kill -9 of the
# wrapper, the worker-mode watchdog must notice the changed ppid and _exit
# within a few seconds; without it the busy-spin keeps the orphan alive
# forever (RED). Skipped on Windows-like shells (the watchdog is POSIX-only).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT}/build/c/codebase-memory-mcp"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    echo "skipping worker watchdog test on Windows"
    exit 0
    ;;
esac

if [[ ! -x "${BINARY}" ]]; then
  echo "missing binary: ${BINARY}" >&2
  exit 2
fi

tmpdir="$(mktemp -d)"
wrapper_pid=""
cleanup() {
  if [[ -s "${tmpdir}/child.pid" ]]; then
    local child_pid
    child_pid="$(cat "${tmpdir}/child.pid" 2>/dev/null || true)"
    [[ -n "${child_pid}" ]] && kill -9 "${child_pid}" 2>/dev/null || true
  fi
  [[ -n "${wrapper_pid}" ]] && kill -9 "${wrapper_pid}" 2>/dev/null || true
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

# Fixture: one good file + one the injector busy-spins on.
mkdir -p "${tmpdir}/repo"
printf 'def good():\n    return 1\n' > "${tmpdir}/repo/good.py"
printf 'def slow():\n    return 2\n' > "${tmpdir}/repo/hang_me.py"

# Wrapper "parent": launches the worker exactly as the supervisor would
# (cli --index-worker … --response-out), records the child PID, then waits.
cat >"${tmpdir}/wrapper.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
"${CBM_BINARY}" cli --index-worker index_repository "${ARGS_JSON}" \
  --response-out "${TMPDIR_PATH}/resp" \
  >/dev/null 2>"${TMPDIR_PATH}/child.err" &
echo "$!" >"${TMPDIR_PATH}/child.pid"
wait
SH
chmod +x "${tmpdir}/wrapper.sh"

CBM_BINARY="${BINARY}" TMPDIR_PATH="${tmpdir}" \
  ARGS_JSON="{\"repo_path\":\"${tmpdir}/repo\"}" \
  CBM_TEST_HANG_ON=hang_me CBM_INDEX_SINGLE_THREAD=1 \
  CBM_INDEX_MARKER_FILE="${tmpdir}/marker" \
  "${tmpdir}/wrapper.sh" &
wrapper_pid=$!

# Wait for the worker PID file to appear.
for _ in {1..50}; do
  [[ -s "${tmpdir}/child.pid" ]] && break
  sleep 0.1
done
if [[ ! -s "${tmpdir}/child.pid" ]]; then
  echo "worker pid file was not written" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  exit 3
fi
child_pid="$(cat "${tmpdir}/child.pid")"

# Sync point: once the marker names the hang file, the worker is provably
# mid-index (busy-spinning in extraction) and long past watchdog installation.
for _ in {1..100}; do
  if [[ -s "${tmpdir}/marker" ]] && grep -q "hang_me" "${tmpdir}/marker"; then
    break
  fi
  sleep 0.1
done
if ! grep -q "hang_me" "${tmpdir}/marker" 2>/dev/null; then
  echo "worker never reached the hang file (marker: $(cat "${tmpdir}/marker" 2>/dev/null || echo '<empty>'))" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  exit 3
fi

# Vacuity guard: the worker must still be ALIVE (busy-spinning) right now —
# it cannot "pass" by having finished before the parent dies.
if ! kill -0 "${child_pid}" 2>/dev/null; then
  echo "worker exited before the supervisor was killed — guard would be vacuous" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  exit 3
fi

# Kill the wrapper parent: the orphaned worker must now self-exit.
kill -9 "${wrapper_pid}"
wait "${wrapper_pid}" 2>/dev/null || true

deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  if ! kill -0 "${child_pid}" 2>/dev/null; then
    echo "ok: worker ${child_pid} exited after supervisor death"
    exit 0
  fi
  # A zombie no longer indexes; kill -0 still reports it until it is reaped,
  # so treat that as a successful exit (same as test_parent_watchdog.sh).
  child_state="$(ps -p "${child_pid}" -o stat= 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${child_state}" == Z* ]]; then
    echo "ok: worker ${child_pid} exited after supervisor death (zombie awaiting reap)"
    exit 0
  fi
  sleep 0.2
done

echo "index worker ${child_pid} survived supervisor death (#845)" >&2
[[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
exit 1
