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
# cannot pass vacuously via the worker simply finishing. The hidden
# --index-worker-single-thread + --index-worker-marker recovery knobs give a
# deterministic "worker is AT the hang file" sync point: the worker writes the
# rel_path it is about to process before touching it. After kill -9 of the
# wrapper, the worker-mode watchdog must notice the changed ppid and kill its
# isolated process group within a few seconds. A test-only worker descendant is
# kept alive in that group to prove teardown is tree-wide rather than merely a
# root-process exit. The recovery knobs are passed as the supervisor's hidden
# worker argv, never by mutating the supervisor environment. Skipped on
# Windows-like shells (the watchdog is POSIX-only).
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

if command -v shasum >/dev/null 2>&1; then
  BUILD_FINGERPRINT="$(shasum -a 256 "${BINARY}" | awk '{print $1}')"
elif command -v sha256sum >/dev/null 2>&1; then
  BUILD_FINGERPRINT="$(sha256sum "${BINARY}" | awk '{print $1}')"
elif command -v openssl >/dev/null 2>&1; then
  BUILD_FINGERPRINT="$(openssl dgst -sha256 "${BINARY}" | awk '{print $NF}')"
else
  echo "no SHA-256 command available for worker build binding" >&2
  exit 2
fi
if [[ ! "${BUILD_FINGERPRINT}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid worker build fingerprint: ${BUILD_FINGERPRINT}" >&2
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
  if [[ -s "${tmpdir}/descendant.pid" ]]; then
    local descendant_pid
    descendant_pid="$(cat "${tmpdir}/descendant.pid" 2>/dev/null || true)"
    [[ -n "${descendant_pid}" ]] && kill -9 "${descendant_pid}" 2>/dev/null || true
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
"${CBM_BINARY}" cli --index-worker \
  --index-worker-build "${BUILD_FINGERPRINT}" \
  index_repository "${ARGS_JSON}" \
  --response-out "${TMPDIR_PATH}/resp" \
  --index-worker-single-thread \
  --index-worker-marker "${TMPDIR_PATH}/marker" \
  >/dev/null 2>"${TMPDIR_PATH}/child.err" &
echo "$!" >"${TMPDIR_PATH}/child.pid"
wait
SH
chmod +x "${tmpdir}/wrapper.sh"

CBM_BINARY="${BINARY}" BUILD_FINGERPRINT="${BUILD_FINGERPRINT}" TMPDIR_PATH="${tmpdir}" \
  ARGS_JSON="{\"repo_path\":\"${tmpdir}/repo\"}" \
  CBM_TEST_HANG_ON=hang_me \
  CBM_TEST_WORKER_DESCENDANT_PID_FILE="${tmpdir}/descendant.pid" \
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

# The validated worker creates its own process group before starting this
# deliberate long-lived descendant. Both must be alive and in that group before
# the wrapper is killed, otherwise the tree assertion would be vacuous.
for _ in {1..50}; do
  [[ -s "${tmpdir}/descendant.pid" ]] && break
  sleep 0.1
done
if [[ ! -s "${tmpdir}/descendant.pid" ]]; then
  echo "worker descendant pid file was not written" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  exit 3
fi
descendant_pid="$(cat "${tmpdir}/descendant.pid")"
if ! kill -0 "${descendant_pid}" 2>/dev/null; then
  echo "worker descendant exited before supervisor death" >&2
  exit 3
fi
child_pgid="$(ps -p "${child_pid}" -o pgid= 2>/dev/null | tr -d '[:space:]')"
descendant_pgid="$(ps -p "${descendant_pid}" -o pgid= 2>/dev/null | tr -d '[:space:]')"
if [[ -z "${child_pgid}" || "${child_pgid}" != "${child_pid}" ||
      "${descendant_pgid}" != "${child_pgid}" ]]; then
  echo "worker tree is not in its expected isolated process group" >&2
  exit 3
fi

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

process_gone_or_zombie() {
  local pid="$1"
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi
  local state
  state="$(ps -p "${pid}" -o stat= 2>/dev/null | tr -d '[:space:]' || true)"
  [[ "${state}" == Z* ]]
}

deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  if process_gone_or_zombie "${child_pid}" && process_gone_or_zombie "${descendant_pid}"; then
    echo "ok: worker ${child_pid} and descendant ${descendant_pid} exited after supervisor death"
    exit 0
  fi
  sleep 0.2
done

echo "index worker tree survived supervisor death (worker=${child_pid}, descendant=${descendant_pid})" >&2
[[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
exit 1
