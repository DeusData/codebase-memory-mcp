#!/usr/bin/env bash
# MCP startup failures must use MCP stdout; hook failures stay fail-open and
# must not leak an MCP JSON-RPC object into the hook protocol.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${CBM_TEST_BINARY:-${ROOT}/build/c/codebase-memory-mcp}"
if [[ ! -x "${BINARY}" && -x "${BINARY}.exe" ]]; then
  BINARY="${BINARY}.exe"
fi
if [[ ! -x "${BINARY}" ]]; then
  echo "missing binary: ${BINARY}" >&2
  exit 2
fi
if ! LC_ALL=C grep -a -q -F 'CBM_TEST_DAEMON_RUNTIME_PARENT' "${BINARY}"; then
  echo "binary lacks the daemon-runtime-parent test seam: ${BINARY}" >&2
  exit 2
fi

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

assert_mcp_failure() {
  local cache_dir="$1"
  local runtime_parent="$2"
  local expected="$3"
  set +e
  CBM_CACHE_DIR="${cache_dir}" CBM_TEST_DAEMON_RUNTIME_PARENT="${runtime_parent}" \
    "${BINARY}" </dev/null >"${tmpdir}/mcp.out" 2>"${tmpdir}/mcp.err"
  local rc=$?
  set -e
  if [[ ${rc} -eq 0 ]] || ! grep -q '"jsonrpc":"2.0"' "${tmpdir}/mcp.out" ||
    ! grep -q "${expected}" "${tmpdir}/mcp.out"; then
    echo "MCP startup failure was not returned as JSON-RPC (rc=${rc})" >&2
    cat "${tmpdir}/mcp.out" "${tmpdir}/mcp.err" >&2
    exit 1
  fi
}

assert_hook_fails_open() {
  local cache_dir="$1"
  local runtime_parent="$2"
  set +e
  printf '{}' | CBM_CACHE_DIR="${cache_dir}" \
    CBM_TEST_DAEMON_RUNTIME_PARENT="${runtime_parent}" \
    "${BINARY}" hook-augment >"${tmpdir}/hook.out" 2>"${tmpdir}/hook.err"
  local rc=$?
  set -e
  if [[ ${rc} -ne 0 ]] || grep -q '"jsonrpc":"2.0"' "${tmpdir}/hook.out" ||
    [[ ! -s "${tmpdir}/hook.err" ]]; then
    echo "hook startup failure blocked the caller or emitted MCP JSON-RPC (rc=${rc})" >&2
    cat "${tmpdir}/hook.out" "${tmpdir}/hook.err" >&2
    exit 1
  fi
}

touch "${tmpdir}/cache-is-a-file" "${tmpdir}/runtime-parent-is-a-file"
mkdir "${tmpdir}/cache"

# Argument validation fails before executable identity preparation.
set +e
CBM_CACHE_DIR="${tmpdir}/cache" "${BINARY}" --tool-profile=invalid \
  </dev/null >"${tmpdir}/mcp.out" 2>"${tmpdir}/mcp.err"
profile_rc=$?
set -e
if [[ ${profile_rc} -eq 0 ]] || ! grep -q '"jsonrpc":"2.0"' "${tmpdir}/mcp.out" ||
  ! grep -q -- '--tool-profile requires' "${tmpdir}/mcp.out"; then
  echo "MCP argument failure was not returned as JSON-RPC (rc=${profile_rc})" >&2
  cat "${tmpdir}/mcp.out" "${tmpdir}/mcp.err" >&2
  exit 1
fi

# Identity preparation fails before the endpoint exists.
assert_mcp_failure "${tmpdir}/cache-is-a-file" "${tmpdir}" "exact executable identity"
assert_hook_fails_open "${tmpdir}/cache-is-a-file" "${tmpdir}"

# Endpoint validation fails after identity preparation succeeds.
assert_mcp_failure "${tmpdir}/cache" "${tmpdir}/runtime-parent-is-a-file" \
  "runtime-parent-is-a-file"
assert_hook_fails_open "${tmpdir}/cache" "${tmpdir}/runtime-parent-is-a-file"

echo "ok: MCP startup failures are visible and hook startup failures remain fail-open"
