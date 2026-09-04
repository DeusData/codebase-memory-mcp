#!/usr/bin/env bash
# test_watcher_disabled.sh — process-level regression for the watcher_enabled
# kill-switch (#335). Requested by the maintainer on PR #1105: the unit test
# (cli_config_watcher_enabled_default_and_persist) only proves the config
# predicate and would still pass if the daemon-side gate were deleted. This
# drives the REAL binary against an isolated cache and proves, at the process
# level, that watcher_enabled=false actually prevents the watcher subsystem from
# initializing:
#   - `watcher.disabled reason=config` IS emitted,
#   - `watcher.start` is ABSENT (the poll thread never runs),
#   - no project registration occurs (`watcher.watch` ABSENT), and
#   - manual index_repository remains available (the MCP tool still serves and
#     indexes with the watcher off).
# A positive control (watcher_enabled=true) proves those signals are real —
# watcher.start + watcher.watch DO appear and watcher.disabled does not — so
# this test fails if the gate is removed.
#
# NO ASSERTION IS DECIDED BY A TIMEOUT. The watcher lives in the daemon
# (src/daemon/host.c), which logs to $CBM_CACHE_DIR/logs/cbm-daemon.log in a
# fixed order: watcher.disabled (host_state_prepare) -> daemon.start -> the
# watcher thread's own watcher.start -> ... -> watcher thread joined ->
# daemon.stop. Each run therefore retires its daemon and waits for that CLOSED
# lifecycle (daemon.start ... daemon.stop) before asserting. Absence over a
# closed lifecycle log is a fact, not a race: if watcher.start is not in a log
# that already contains daemon.stop, the thread never ran. Every wait is a
# bounded poll on an asserted state (the soak harness's wait_for_daemon_stop
# doctrine, scripts/soak-test.sh), and exhausting a poll is a FAILURE with the
# reason printed — never a silently-passing sleep.
#
# Skipped on Windows-like shells (uses POSIX process control + git fixture).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${CBM_TEST_BINARY:-${ROOT}/build/c/codebase-memory-mcp}"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) echo "skipping watcher_disabled test on Windows"; exit 0 ;;
esac
[[ -x "${BINARY}" ]] || { echo "missing binary: ${BINARY}" >&2; exit 2; }
command -v git >/dev/null 2>&1 || { echo "git required for fixture" >&2; exit 2; }

work="$(mktemp -d)"

# Every run gets its own CBM_CACHE_DIR, and the daemon endpoint is derived from
# it, so these daemons are private to the test and can never touch a developer's
# real one. Retire them on any exit path regardless.
cleanup() {
  local cache
  for cache in "${work}"/cache-*; do
    [[ -d "${cache}" ]] || continue
    CBM_CACHE_DIR="${cache}" "${BINARY}" daemon stop >/dev/null 2>&1 || true
  done
  rm -rf "${work}"
}
trap cleanup EXIT

# --- tiny git fixture so indexing is fast + deterministic ---------------------
repo="${work}/repo"
mkdir -p "${repo}"
cat >"${repo}/sample.c" <<'EOF'
int add(int a, int b) { return a + b; }
int main(void) { return add(1, 2); }
EOF
git -C "${repo}" init -q
git -C "${repo}" -c user.email=t@example.com -c user.name=t add -A
git -C "${repo}" -c user.email=t@example.com -c user.name=t commit -q -m init

INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"watcher-test","version":"1.0"}}}'
INITED='{"jsonrpc":"2.0","method":"notifications/initialized"}'

CURRENT_RUN="(setup)"

fail() {
  echo "FAIL [${CURRENT_RUN}]: $*" >&2
  local log
  for log in "${work}"/cache-*/logs/cbm-daemon.log; do
    [[ -f "${log}" ]] || continue
    echo "----- ${log} (watcher/daemon lines) -----" >&2
    grep -E 'msg=(watcher|daemon)\.' "${log}" >&2 || true
  done
  exit 1
}

# wait_for <file> <extended-regex> <label>
# Bounded poll on an asserted state: returns as soon as the state is observed,
# and a exhausted budget is a failure with the reason — never a fixed sleep that
# lets the assertion proceed on an unobserved state. 30s ceiling mirrors
# scripts/soak-test.sh (attempts=300 x 0.1s): the wait sits above the worst case
# so a slow, loaded CI runner is not mistaken for a broken gate.
wait_for() {
  local file="$1" pattern="$2" label="$3"
  local attempts=300
  while [ "${attempts}" -gt 0 ]; do
    if [ -f "${file}" ] && grep -qE "${pattern}" "${file}"; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 0.1
  done
  fail "timed out after 30s waiting for ${label} in ${file}"
}

log_has() { grep -qE "$2" "$1"; }

# wait_for_project_db <cache_dir>
# Registration is not reachable until the project DB exists: the daemon only
# refreshes a session's watch once application_regular_db_exists(project) is
# true (application.c application_refresh_watch_locked). Waiting on the DB is
# therefore waiting on registration's own precondition — after this returns, a
# missing watcher.watch is the gate's doing, not a session that ended too soon.
wait_for_project_db() {
  local cache="$1" attempts=300 db
  while [ "${attempts}" -gt 0 ]; do
    for db in "${cache}"/*.db; do
      # An unmatched glob stays literal, and _config.db is the settings store,
      # not a project graph.
      case "${db}" in
        */_config.db) continue ;;
        *'*.db') continue ;;
      esac
      if [ -f "${db}" ]; then
        return 0
      fi
    done
    attempts=$((attempts - 1))
    sleep 0.1
  done
  fail "timed out after 30s waiting for the project DB under ${cache}"
}

# run_session <cache_dir> <outfile> [extra rpc line ...]
# Drives the real binary in MCP stdio mode with cwd = the fixture repo (so the
# session root is derived from it), holding stdin open via a FIFO until the LAST
# request has been answered on stdout — an asserted state, not an interval —
# then closing it (EOF -> clean shutdown). Sets DAEMON_LOG for the caller.
#
# Registration happens asynchronously, after indexing, so a closed lifecycle
# alone would not carry a NEGATIVE registration assertion — the session could
# simply have ended before registration was ever reachable. Two optional
# pre-close waits close that gap, both observed while the session is still live:
#   PRE_CLOSE_DB=1        wait for the project DB, which is registration's own
#                         precondition (see wait_for_project_db).
#   PRE_CLOSE_WAIT=<re>   wait for a daemon-log pattern (with PRE_CLOSE_LABEL),
#                         used by the positive control to observe registration
#                         actually happening in this session shape.
run_session() {
  local cache="$1" outf="$2"; shift 2
  local fifo="${work}/stdin.fifo"
  rm -f "${fifo}"; mkfifo "${fifo}"
  DAEMON_LOG="${cache}/logs/cbm-daemon.log"

  ( cd "${repo}" && CBM_CACHE_DIR="${cache}" "${BINARY}" <"${fifo}" \
      >"${outf}" 2>"${cache}.client.err" ) &
  local server_pid=$!

  exec 3>"${fifo}"
  printf '%s\n' "${INIT}" >&3
  printf '%s\n' "${INITED}" >&3
  local line last_id=1
  for line in "$@"; do
    printf '%s\n' "${line}" >&3
    last_id=$((last_id + 1))
  done
  # Wait for the final response before closing stdin, so shutdown never races
  # the work under test.
  wait_for "${outf}" "\"id\":${last_id}[,}]" "the id=${last_id} JSON-RPC response"
  if [ -n "${PRE_CLOSE_DB:-}" ]; then
    wait_for_project_db "${cache}"
  fi
  if [ -n "${PRE_CLOSE_WAIT:-}" ]; then
    wait_for "${DAEMON_LOG}" "${PRE_CLOSE_WAIT}" "${PRE_CLOSE_LABEL:-${PRE_CLOSE_WAIT}}"
  fi
  exec 3>&-
  wait "${server_pid}" || true

  # Retire the daemon and wait for the lifecycle to close, so the assertions
  # below read a complete log rather than a snapshot of one still being written.
  CBM_CACHE_DIR="${cache}" "${BINARY}" daemon stop >/dev/null 2>&1 || true
  wait_for "${DAEMON_LOG}" 'msg=daemon\.start( |$)' "the daemon to record its start"
  wait_for "${DAEMON_LOG}" 'msg=daemon\.stop( |$)' "the daemon lifecycle to close"
}

# =============================================================================
# Run 1 — DISABLED + manual index_repository (auto_index off, no contention).
#   Proves: gate fires, watcher never starts, and the manual MCP tool still
#   serves + indexes with the watcher off.
# =============================================================================
CURRENT_RUN="run 1: disabled + manual index_repository"
c_offm="${work}/cache-offm"
CBM_CACHE_DIR="${c_offm}" "${BINARY}" config set watcher_enabled false >/dev/null
CBM_CACHE_DIR="${c_offm}" "${BINARY}" config set auto_index false >/dev/null
run_session "${c_offm}" "${work}/offm.out" \
  "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"index_repository\",\"arguments\":{\"repo_path\":\"${repo}\",\"mode\":\"fast\"}}}"

log_has "${DAEMON_LOG}" 'msg=watcher\.disabled( |$)' \
  || fail "watcher.disabled not emitted when disabled"
log_has "${DAEMON_LOG}" 'msg=watcher\.disabled .*reason=config' \
  || fail "watcher.disabled emitted without reason=config"
! log_has "${DAEMON_LOG}" 'msg=watcher\.start( |$)' \
  || fail "watcher.start present when disabled (the poll thread ran)"
! log_has "${DAEMON_LOG}" 'msg=watcher\.watch( |$)' \
  || fail "watcher.watch (registration) present when disabled"

resp="$(grep '"id":2' "${work}/offm.out" | head -1)"
[ -n "${resp}" ] || fail "no index_repository response (tool unavailable when watcher off)"
case "${resp}" in
  *'"error"'*) fail "index_repository returned an error with the watcher off: ${resp}" ;;
esac
case "${resp}" in
  *nodes*) : ;;
  *) fail "index_repository response reports no indexed nodes: ${resp}" ;;
esac
echo "ok: disabled — watcher.disabled emitted, no watcher.start/watch, manual index_repository served"

# =============================================================================
# Run 2 — DISABLED + auto_index on. Same-config apples-to-apples for the
#   registration axis: indexing still runs, but NO registration happens.
# =============================================================================
CURRENT_RUN="run 2: disabled + auto_index"
c_offa="${work}/cache-offa"
CBM_CACHE_DIR="${c_offa}" "${BINARY}" config set watcher_enabled false >/dev/null
CBM_CACHE_DIR="${c_offa}" "${BINARY}" config set auto_index true >/dev/null
# Hold the session open until auto-indexing has produced the project DB, so
# registration's precondition is satisfied and the absence below is the gate.
PRE_CLOSE_DB=1
run_session "${c_offa}" "${work}/offa.out"
PRE_CLOSE_DB=""

log_has "${DAEMON_LOG}" 'msg=watcher\.disabled( |$)' \
  || fail "watcher.disabled not emitted (auto_index run)"
# Contract point 3 is broader than the manual tool: turning the watcher off must
# not turn auto-indexing off with it. The DB above proves the pipeline ran.
log_has "${DAEMON_LOG}" 'msg=daemon\.start( |$)' \
  || fail "daemon did not start with the watcher disabled"
! log_has "${DAEMON_LOG}" 'msg=watcher\.start( |$)' \
  || fail "watcher.start present when disabled (auto_index run)"
! log_has "${DAEMON_LOG}" 'msg=watcher\.watch( |$)' \
  || fail "project registered with the watcher when disabled"
echo "ok: disabled+auto_index — auto-index still ran, no watcher.start, no registration"

# =============================================================================
# Run 3 — ENABLED positive control (auto_index on). Proves the signals above
#   are real: the watcher starts AND registers the session project — so their
#   absence in Runs 1-2 is meaningful, and removing the gate fails this test.
# =============================================================================
CURRENT_RUN="run 3: enabled positive control"
c_on="${work}/cache-on"
CBM_CACHE_DIR="${c_on}" "${BINARY}" config set watcher_enabled true >/dev/null
CBM_CACHE_DIR="${c_on}" "${BINARY}" config set auto_index true >/dev/null
# The control must observe registration actually happening in this exact session
# shape — that is what makes its absence in runs 1-2 evidence rather than noise.
PRE_CLOSE_WAIT='msg=watcher\.watch( |$)'
PRE_CLOSE_LABEL="the session project to register with the watcher"
run_session "${c_on}" "${work}/on.out"
PRE_CLOSE_WAIT=""

log_has "${DAEMON_LOG}" 'msg=watcher\.start( |$)' \
  || fail "watcher.start absent when enabled (the watcher never started)"
log_has "${DAEMON_LOG}" 'msg=watcher\.watch( |$)' \
  || fail "no project registration when enabled (control failed)"
! log_has "${DAEMON_LOG}" 'msg=watcher\.disabled( |$)' \
  || fail "watcher.disabled present when enabled"
echo "ok: enabled — watcher.start emitted and session project registered"

echo "PASS: watcher_enabled kill-switch process regression (#335)"
