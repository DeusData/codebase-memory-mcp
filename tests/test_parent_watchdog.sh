#!/usr/bin/env bash
# test_parent_watchdog.sh — regression guard for the parent-death watchdog.
# Distilled from #407 (fixes #406): when the process that launched the stdio
# MCP server dies, the orphaned server must exit on its own rather than linger
# forever blocked on stdin.
#
# Strategy: launch the binary under a wrapper "parent" process (stdin kept open
# via a FIFO so the server doesn't see EOF), record the child's PID, then kill
# the wrapper. The watchdog should notice the changed ppid and exit within a
# few seconds. On Windows (MSYS2 shells) the same tree applies: the wrapper is
# an MSYS bash process, the child a native binary whose parent handle the
# watchdog waits on; killing the wrapper terminates that parent process (#914).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${CBM_TEST_BINARY:-${ROOT}/build/c/codebase-memory-mcp}"

windows_mode=0
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    windows_mode=1
    ;;
esac

if [[ ! -x "${BINARY}" ]]; then
  echo "missing binary: ${BINARY}" >&2
  exit 2
fi

# shellcheck source=../scripts/test-runtime.sh
source "${ROOT}/scripts/test-runtime.sh"
cbm_test_runtime_init
tmpdir="${CBM_TEST_RUNTIME_ROOT}"
wrapper_pid=""
writer_pid=""

kill_hard() {
  # MSYS signal delivery does not reliably terminate the Windows process
  # behind an MSYS pid (observed: a wrapper bash kept running after kill -9,
  # so a child's real parent never actually died). Force-kill through
  # TerminateProcess on the Windows pid as well. The winpid must be resolved
  # BEFORE the msys kill: kill -9 removes the pid from the MSYS process table
  # immediately, and the mapping would be lost. `ps -W` exists only on MSYS —
  # under `set -o pipefail` a failing ps would abort this helper on POSIX —
  # so the pipeline is guarded and simply yields an empty winpid elsewhere.
  local pid="$1" winpid
  winpid="$(ps -W 2>/dev/null | awk -v m="${pid}" '$1==m {print $4; exit}' || true)"
  kill -9 "${pid}" 2>/dev/null || true
  [[ -n "${winpid}" ]] && taskkill //F //PID "${winpid}" >/dev/null 2>&1 || true
}

cleanup() {
  if [[ -s "${tmpdir}/child.pid" ]]; then
    local child_pid
    child_pid="$(cat "${tmpdir}/child.pid" 2>/dev/null || true)"
    [[ -n "${child_pid}" ]] && kill_hard "${child_pid}" || true
  fi
  # On Windows the pipe writer is an orphaned helper that outlives the wrapper
  # by design (it holds stdin open); only the test knows its PID.
  if [[ -n "${writer_pid}" ]]; then
    kill_hard "${writer_pid}" || true
  fi
  [[ -n "${wrapper_pid}" ]] && kill_hard "${wrapper_pid}" || true
  cbm_test_runtime_cleanup "${BINARY}"
}
trap cleanup EXIT

if (( windows_mode )); then
  # MSYS FIFOs are not readable by native Windows binaries (the server blocks
  # forever), so the stdin-holder is an anonymous PIPE instead: a writer helper
  # forwards the initialize request once the test drops it into a file, then
  # holds the pipe's write end open forever. Killing the wrapper orphans the
  # writer too, so stdin never sees EOF — the child's exit can only come from
  # the parent-death watchdog.
  cat >"${tmpdir}/wrapper.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
echo $$ >"${TMPDIR_PATH}/writer.pid"
(
  for _ in {1..300}; do
    if [[ -s "${REQ_FILE}" ]]; then
      cat "${REQ_FILE}"
      exec sleep 3600
    fi
    sleep 0.1
  done
) | "${CBM_BINARY}" >"${TMPDIR_PATH}/child.out" 2>"${TMPDIR_PATH}/child.err" &
echo "$!" >"${TMPDIR_PATH}/child.pid"
wait
SH
  chmod +x "${tmpdir}/wrapper.sh"
  : >"${tmpdir}/request.json"

  CBM_BINARY="${BINARY}" REQ_FILE="${tmpdir}/request.json" TMPDIR_PATH="${tmpdir}" \
    "${tmpdir}/wrapper.sh" &
  wrapper_pid=$!
  for _ in {1..50}; do
    [[ -s "${tmpdir}/writer.pid" ]] && break
    sleep 0.1
  done
  [[ -s "${tmpdir}/writer.pid" ]] && writer_pid="$(cat "${tmpdir}/writer.pid")"
else
  # Wrapper "parent": opens the FIFO read-write so it stays open, launches the
  # MCP server with that FIFO as stdin, records the child PID, then waits.
  cat >"${tmpdir}/wrapper.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec 3<>"${FIFO}"
"${CBM_BINARY}" <&3 >"${TMPDIR_PATH}/child.out" 2>"${TMPDIR_PATH}/child.err" &
echo "$!" >"${TMPDIR_PATH}/child.pid"
wait
SH
  chmod +x "${tmpdir}/wrapper.sh"
  mkfifo "${tmpdir}/stdin"

  CBM_BINARY="${BINARY}" FIFO="${tmpdir}/stdin" TMPDIR_PATH="${tmpdir}" \
    "${tmpdir}/wrapper.sh" &
  wrapper_pid=$!
fi

# Wait for the child PID file to appear.
for _ in {1..50}; do
  [[ -s "${tmpdir}/child.pid" ]] && break
  sleep 0.1
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

# Complete one MCP request before killing the parent. A response proves that
# the frontend reached its stdio loop after installing the parent watchdog.
# The old mem.init log sync point belonged to the pre-daemon architecture: the
# shared daemon now owns memory initialization, so a frontend need not emit it.
if (( windows_mode )); then
  request_target="${tmpdir}/request.json"
else
  request_target="${tmpdir}/stdin"
fi
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"parent-watchdog-test","version":"1.0"}}}' \
  >"${request_target}"
for _ in {1..150}; do
  if [[ -s "${tmpdir}/child.out" ]] &&
    grep -Eq '"id"[[:space:]]*:[[:space:]]*1' "${tmpdir}/child.out"; then
    break
  fi
  sleep 0.1
done
if ! grep -Eq '"id"[[:space:]]*:[[:space:]]*1' "${tmpdir}/child.out" 2>/dev/null; then
  echo "child did not reach watchdog-ready startup point" >&2
  [[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
  [[ -s "${tmpdir}/child.out" ]] && cat "${tmpdir}/child.out" >&2
  exit 3
fi

# Kill the wrapper parent: the orphaned child must now self-exit.
if (( windows_mode )); then
  # Two Windows-specific traps make "kill -9 $wrapper_pid" wrong here:
  #  1. MSYS kill -9 does not reliably terminate the Windows process behind
  #     an MSYS pid (a wrapper bash keeps running), and
  #  2. the wrapper's background PIPELINE puts an intermediate subshell bash
  #     between wrapper and server, and the watchdog watches the
  #     Windows-physical parent (Toolhelp ParentProcessId) — that subshell.
  # Resolve the child's actual Windows parent and TerminateProcess exactly
  # that one, mirroring a force-killed MCP client. The wrapper is NOT waited
  # on yet: its wait covers the whole pipeline job, and the stdin-holding
  # writer must survive until the child's exit is observed, or stdin would
  # EOF and the test could pass without the watchdog doing anything.
  child_winpid="$(ps -W 2>/dev/null | awk -v m="${child_pid}" '$1==m {print $4; exit}')"
  if [[ -z "${child_winpid}" ]]; then
    echo "child windows pid not found for msys pid ${child_pid}" >&2
    exit 3
  fi
  parent_winpid="$(powershell.exe -NoProfile -Command \
    "(Get-CimInstance Win32_Process -Filter \"ProcessId=${child_winpid}\").ParentProcessId" \
    2>/dev/null | tr -d '[:space:]')"
  if [[ -z "${parent_winpid}" || ! "${parent_winpid}" =~ ^[0-9]+$ ]]; then
    echo "could not resolve the child's windows parent pid" >&2
    exit 3
  fi
  taskkill //F //PID "${parent_winpid}" >/dev/null 2>&1 || true
else
  kill -9 "${wrapper_pid}"
  wait "${wrapper_pid}" 2>/dev/null || true
fi

deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  if ! kill -0 "${child_pid}" 2>/dev/null; then
    echo "ok: child ${child_pid} exited after parent death"
    exit 0
  fi
  # A zombie no longer holds stdin or runs the MCP loop; kill -0 still reports
  # it until launchd/test parent reaps it, so treat that as a successful exit.
  # Windows has no zombie state — an exited process simply disappears from
  # kill -0 — so this probe is POSIX-only.
  if [[ "${windows_mode}" -eq 0 ]]; then
    child_state="$(ps -p "${child_pid}" -o stat= 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ "${child_state}" == Z* ]]; then
      echo "ok: child ${child_pid} exited after parent death (zombie awaiting reap)"
      exit 0
    fi
  fi
  sleep 0.2
done

echo "codebase-memory-mcp child ${child_pid} survived parent death" >&2
[[ -s "${tmpdir}/child.err" ]] && cat "${tmpdir}/child.err" >&2
exit 1
