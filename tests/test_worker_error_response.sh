#!/usr/bin/env bash
# A supervised worker that produces a valid MCP error response is healthy.
# The CLI may exit nonzero when presenting that error to a human, but the worker
# transport must exit zero so its parent reads the response instead of inventing
# an exit_nonzero "crashed on a file" diagnosis.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# scripts/test.sh builds into $BUILD_DIR, which is NOT build/c on every leg (the
# Linux containers use build/linux-arm64 / build/linux-amd64). Honour the binary
# the caller built, exactly as test_parent_watchdog.sh and test_worker_watchdog.sh
# do; the build/c default keeps a bare manual invocation working.
BINARY="${CBM_TEST_BINARY:-${ROOT}/build/c/codebase-memory-mcp}"
if [[ ! -x "${BINARY}" && -x "${BINARY}.exe" ]]; then
  BINARY="${BINARY}.exe"
fi

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
case "$(uname -s)" in
  Darwin) runtime_root="/private/tmp" ;;
  *) runtime_root="/tmp" ;;
esac
runtime_parent="$(mktemp -d "${runtime_root}/cbm-worker-error-runtime.XXXXXX")"
cleanup() {
  CBM_TEST_DAEMON_RUNTIME_PARENT="${runtime_parent}" \
  CBM_CACHE_DIR="${tmpdir}/cache-supervisor" \
    "${BINARY}" daemon stop >/dev/null 2>&1 || true
  rm -rf "${tmpdir}"
  rm -rf "${runtime_parent}"
}
trap cleanup EXIT

missing="${tmpdir}/repository-does-not-exist"
response="${tmpdir}/worker.response"
args="{\"repo_path\":\"${missing}\",\"mode\":\"fast\"}"

if ! CBM_TEST_DAEMON_RUNTIME_PARENT="${runtime_parent}" \
  CBM_CACHE_DIR="${tmpdir}/cache-worker" \
  "${BINARY}" cli --index-worker \
  --index-worker-build "${BUILD_FINGERPRINT}" \
  index_repository "${args}" \
  --response-out "${response}" >"${tmpdir}/worker.out" 2>"${tmpdir}/worker.err"; then
  echo "worker treated a delivered MCP error as a process failure" >&2
  cat "${tmpdir}/worker.err" >&2
  exit 1
fi

if [[ ! -s "${response}" ]] || ! grep -q 'Pipeline failed' "${response}"; then
  echo "worker did not deliver the underlying pipeline error" >&2
  cat "${response}" 2>/dev/null >&2 || true
  exit 1
fi

set +e
CBM_TEST_DAEMON_RUNTIME_PARENT="${runtime_parent}" \
  CBM_CACHE_DIR="${tmpdir}/cache-supervisor" \
  "${BINARY}" cli index_repository --repo-path "${missing}" --mode fast \
  >"${tmpdir}/supervisor.out" 2>"${tmpdir}/supervisor.err"
cli_rc=$?
set -e

if [[ ${cli_rc} -eq 0 ]]; then
  echo "human-facing CLI unexpectedly accepted an indexing error" >&2
  exit 1
fi
if grep -q 'crashed on a file' "${tmpdir}/supervisor.out" "${tmpdir}/supervisor.err"; then
  echo "supervisor replaced a valid tool error with a false crash diagnosis" >&2
  exit 1
fi
if ! grep -q 'Pipeline failed' "${tmpdir}/supervisor.out" "${tmpdir}/supervisor.err"; then
  echo "supervisor did not preserve the worker's pipeline error" >&2
  cat "${tmpdir}/supervisor.out" >&2
  cat "${tmpdir}/supervisor.err" >&2
  exit 1
fi

echo "ok: worker MCP errors remain tool errors, not process crashes"
