#!/usr/bin/env bash
# test_parent_watchdog.sh — regression guard for the parent-death watchdog.
# Distilled from #407 (fixes #406): when the process that launched the stdio
# MCP server dies, the orphaned server must exit on its own rather than linger
# forever blocked on stdin.
#
# Strategy: launch the binary under a wrapper "parent" process (stdin kept open
# via a FIFO so the server doesn't see EOF), record the child's PID, then kill
# the wrapper. The watchdog should notice the changed ppid and exit within a
# few seconds. Skipped on Windows-like shells (the watchdog is POSIX-only).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT}/build/c/codebase-memory-mcp"
CHILD_START_ATTEMPTS=50
CHILD_START_POLL_SECONDS=0.1
CHILD_READY_LOG_PATTERN='msg=mem.init'
WATCHDOG_EXIT_TIMEOUT_SECONDS=6
WATCHDOG_EXIT_POLL_SECONDS=0.2

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    echo "skipping parent watchdog test on Windows"
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
    [[ -n "${child_pid}" ]] && kill "${child_pid}" 2>/dev/null || true
  fi
  [[ -n "${wrapper_pid}" ]] && kill "${wrapper_pid}" 2>/dev/null || true
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

# Wrapper "parent": opens the FIFO read-write so it stays open, launches the
# MCP server with that FIFO as stdin, records the child PID, then waits.
cat >"${tmpdir}/wrapper.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec 3<>"${FIFO}"
CBM_LOG_LEVEL=info "${CBM_BINARY}" <&3 >/dev/null 2>"${TMPDIR_PATH}/child.err" &
echo "$!" >"${TMPDIR_PATH}/child.pid"
wait
SH
chmod +x "${tmpdir}/wrapper.sh"
mkfifo "${tmpdir}/stdin"

CBM_BINARY="${BINARY}" FIFO="${tmpdir}/stdin" TMPDIR_PATH="${tmpdir}" \
  "${tmpdir}/wrapper.sh" &
wrapper_pid=$!

# Wait for the child PID file to appear.
for ((attempt = 0; attempt < CHILD_START_ATTEMPTS; attempt++)); do
  [[ -s "${tmpdir}/child.pid" ]] && break
  sleep "${CHILD_START_POLL_SECONDS}"
done

if [[ ! -s "${tmpdir}/child.pid" ]]; then
  echo "child pid file was not written" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  exit 3
fi

child_pid="$(cat "${tmpdir}/child.pid")"
if ! kill -0 "${child_pid}" 2>/dev/null; then
  echo "child did not start" >&2
  exit 3
fi

# Publishing the child PID happens immediately after fork, before main() has
# necessarily captured its initial parent PID. Killing the wrapper at that
# point races startup: the child can observe ppid==1 and deliberately disable
# the watchdog. mem.init is emitted after watchdog creation, so it is the
# readiness barrier for a deterministic parent-death assertion.
for ((attempt = 0; attempt < CHILD_START_ATTEMPTS; attempt++)); do
  grep -q "${CHILD_READY_LOG_PATTERN}" "${tmpdir}/child.err" 2>/dev/null && break
  if ! kill -0 "${child_pid}" 2>/dev/null; then
    echo "child exited before watchdog initialization" >&2
    [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
    exit 3
  fi
  sleep "${CHILD_START_POLL_SECONDS}"
done

if ! grep -q "${CHILD_READY_LOG_PATTERN}" "${tmpdir}/child.err" 2>/dev/null; then
  echo "child did not initialize the parent watchdog" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  exit 3
fi

# Kill the wrapper parent: the orphaned child must now self-exit.
kill -9 "${wrapper_pid}"
wait "${wrapper_pid}" 2>/dev/null || true

deadline=$((SECONDS + WATCHDOG_EXIT_TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if ! kill -0 "${child_pid}" 2>/dev/null; then
    echo "ok: child ${child_pid} exited after parent death"
    exit 0
  fi
  sleep "${WATCHDOG_EXIT_POLL_SECONDS}"
done

echo "codebase-memory-mcp child ${child_pid} survived parent death" >&2
[[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
exit 1
