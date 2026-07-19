#!/usr/bin/env bash
# test_watcher_disabled.sh — process-level regression for the watcher_enabled
# kill-switch (#335). Requested by the maintainer on PR #1105: the unit test
# (cli_config_watcher_enabled_default_and_persist) only proves the config
# predicate and would still pass if main()'s gate were deleted. This drives the
# REAL server binary with an isolated cache/config and proves, at the process
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
# Skipped on Windows-like shells (uses POSIX process control + git fixture).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT}/build/c/codebase-memory-mcp"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) echo "skipping watcher_disabled test on Windows"; exit 0 ;;
esac
[[ -x "${BINARY}" ]] || { echo "missing binary: ${BINARY}" >&2; exit 2; }
command -v git >/dev/null 2>&1 || { echo "git required for fixture" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

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

# Force in-process indexing so auto-index + registration logs land in THIS
# process's stderr (no supervised-worker subprocess timing to chase).
export CBM_INDEX_SUPERVISOR=0

INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"watcher-test","version":"1.0"}}}'
INITED='{"jsonrpc":"2.0","method":"notifications/initialized"}'

# run_session <cache_dir> <errfile> <outfile> <wait_s> <rpc-line>...
# Feeds newline-delimited JSON-RPC to the server (cwd = the fixture repo so the
# session root is derived from it), holds stdin open for <wait_s> so async
# auto-index / watcher-thread logging can land, then closes it (EOF → clean
# server shutdown).
run_session() {
  local cache="$1" errf="$2" outf="$3" wait_s="$4"; shift 4
  local rpc="${work}/rpc"
  : >"${rpc}"
  local line
  for line in "$@"; do printf '%s\n' "${line}" >>"${rpc}"; done
  ( cat "${rpc}"; sleep "${wait_s}" ) \
    | ( cd "${repo}" && CBM_CACHE_DIR="${cache}" "${BINARY}" >"${outf}" 2>"${errf}" ) \
    || true
}

fail() {
  echo "FAIL: $*" >&2
  echo "----- off-manual stderr -----" >&2; cat "${work}/offm.err" 2>/dev/null | grep -E 'msg=watcher' >&2 || true
  echo "----- off-auto stderr -----"   >&2; cat "${work}/offa.err" 2>/dev/null | grep -E 'msg=watcher|autoindex' >&2 || true
  echo "----- on-auto stderr -----"    >&2; cat "${work}/on.err"   2>/dev/null | grep -E 'msg=watcher|autoindex' >&2 || true
  exit 1
}

# =============================================================================
# Run 1 — DISABLED + manual index_repository (auto_index off, no contention).
#   Proves: gate fires, watcher never starts, and the manual MCP tool still
#   serves + indexes with the watcher off.
# =============================================================================
c_offm="${work}/cache-offm"
CBM_CACHE_DIR="${c_offm}" "${BINARY}" config set watcher_enabled false >/dev/null
run_session "${c_offm}" "${work}/offm.err" "${work}/offm.out" 6 \
  "${INIT}" "${INITED}" \
  "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"index_repository\",\"arguments\":{\"repo_path\":\"${repo}\",\"mode\":\"fast\"}}}"

grep -qE 'msg=watcher\.disabled( |$)' "${work}/offm.err" \
  && grep -q 'reason=config' "${work}/offm.err" || fail "watcher.disabled not emitted when disabled"
if grep -qE 'msg=watcher\.start( |$)' "${work}/offm.err"; then fail "watcher.start present when disabled"; fi
if grep -qE 'msg=watcher\.watch( |$)' "${work}/offm.err"; then fail "watcher.watch (registration) present when disabled"; fi
grep -q '"id":2' "${work}/offm.out" || fail "no index_repository response (tool unavailable when watcher off)"
grep -qE 'msg=pass\.done|persist_hashes' "${work}/offm.err" || fail "manual index_repository did not run the index pipeline"
echo "ok: disabled — watcher.disabled emitted, no watcher.start/watch, manual index_repository served"

# =============================================================================
# Run 2 — DISABLED + auto_index on. Same-config apples-to-apples for the
#   registration axis: indexing still runs, but NO registration happens.
# =============================================================================
c_offa="${work}/cache-offa"
CBM_CACHE_DIR="${c_offa}" "${BINARY}" config set watcher_enabled false >/dev/null
CBM_CACHE_DIR="${c_offa}" "${BINARY}" config set auto_index true >/dev/null
run_session "${c_offa}" "${work}/offa.err" "${work}/offa.out" 8 "${INIT}" "${INITED}"

grep -qE 'msg=watcher\.disabled( |$)' "${work}/offa.err" || fail "watcher.disabled not emitted (auto_index run)"
if grep -qE 'msg=watcher\.start( |$)' "${work}/offa.err"; then fail "watcher.start present when disabled (auto_index run)"; fi
if grep -qE 'msg=watcher\.watch( |$)' "${work}/offa.err"; then fail "project registered with watcher when disabled"; fi
grep -qE 'msg=autoindex\.(done|skip)' "${work}/offa.err" || fail "auto-index did not run with watcher off"
echo "ok: disabled+auto_index — indexing ran, no watcher.start, no registration"

# =============================================================================
# Run 3 — ENABLED positive control (auto_index on). Proves the signals above
#   are real: the watcher starts AND registers the session project — so their
#   absence in Runs 1-2 is meaningful, and removing the gate fails this test.
# =============================================================================
c_on="${work}/cache-on"
CBM_CACHE_DIR="${c_on}" "${BINARY}" config set watcher_enabled true >/dev/null
CBM_CACHE_DIR="${c_on}" "${BINARY}" config set auto_index true >/dev/null
run_session "${c_on}" "${work}/on.err" "${work}/on.out" 8 "${INIT}" "${INITED}"

grep -qE 'msg=watcher\.start( |$)' "${work}/on.err" || fail "watcher.start absent when enabled (watcher never started)"
if grep -qE 'msg=watcher\.disabled( |$)' "${work}/on.err"; then fail "watcher.disabled present when enabled"; fi
grep -qE 'msg=watcher\.watch( |$)' "${work}/on.err" || fail "no project registration when enabled (control failed)"
echo "ok: enabled — watcher.start emitted and session project registered"

echo "PASS: watcher_enabled kill-switch process regression (#335)"
