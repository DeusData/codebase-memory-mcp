#!/usr/bin/env bash
# A supervised index worker (`cli --index-worker index_repository ...`) runs in
# the DAEMON's environment, not the requesting client's. The daemon has already
# admitted the request under the client's session policy and re-executes the
# worker with the canonical repo_path in its args, so the worker must scope its
# own workspace boundary to that request. Before the fix it built an unscoped
# server, fell back to the process-wide CBM_ALLOWED_ROOT it inherited from the
# daemon starter, and refused every admitted session outside that root with
# "... is outside the allowed root". A missing repo_path must fail closed: a
# worker never indexes under an ambient policy.
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

# shellcheck source=../scripts/test-runtime.sh
source "${ROOT}/scripts/test-runtime.sh"
cbm_test_runtime_init
tmpdir="${CBM_TEST_RUNTIME_ROOT}"
cleanup() {
  cbm_test_runtime_cleanup "${BINARY}"
}
trap cleanup EXIT

# Product-facing path, the way the daemon hands it to the worker: canonical on
# POSIX, mixed-style on a native Windows binary (test-runtime.sh's convention).
product_path() {
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) cygpath -m "$1" ;;
    *) (cd "$1" && pwd -P) ;;
  esac
}

# Root A is what the daemon's starter allowed; the admitted session lives under
# root B. The two share no prefix.
mkdir -p "${tmpdir}/root-a" "${tmpdir}/root-b/tiny"
printf 'int tiny_main(void) { return 0; }\n' >"${tmpdir}/root-b/tiny/tiny.c"
root_a="$(product_path "${tmpdir}/root-a")"
repo="$(product_path "${tmpdir}/root-b/tiny")"

run_worker() {
  local args="$1" response="$2" out="$3" err="$4"
  CBM_ALLOWED_ROOT="${root_a}" \
    "${BINARY}" cli --index-worker \
    --index-worker-build "${BUILD_FINGERPRINT}" \
    index_repository "${args}" \
    --response-out "${response}" >"${out}" 2>"${err}"
}

response="${tmpdir}/scoped.response"
if ! run_worker "{\"repo_path\":\"${repo}\",\"mode\":\"fast\"}" "${response}" \
  "${tmpdir}/scoped.out" "${tmpdir}/scoped.err"; then
  echo "worker exited nonzero for an admitted request" >&2
  cat "${tmpdir}/scoped.err" >&2
  exit 1
fi
if [[ ! -s "${response}" ]]; then
  echo "worker delivered no response" >&2
  exit 1
fi
if grep -q 'outside the allowed root' "${response}"; then
  echo "worker re-decided the workspace boundary from the daemon environment" >&2
  cat "${response}" >&2
  exit 1
fi
if ! grep -q '"status":"indexed"' "${response}"; then
  echo "worker did not index the admitted repository" >&2
  cat "${response}" >&2
  exit 1
fi

# Fail closed: no repo_path means no request scope, so no indexing at all.
unscoped="${tmpdir}/unscoped.response"
if run_worker '{"mode":"fast"}' "${unscoped}" "${tmpdir}/unscoped.out" "${tmpdir}/unscoped.err"; then
  echo "worker ran without a request workspace scope" >&2
  exit 1
fi
if [[ -s "${unscoped}" ]] || ! grep -q 'request workspace scope invalid' "${tmpdir}/unscoped.err"; then
  echo "worker without repo_path did not fail closed on scope" >&2
  cat "${tmpdir}/unscoped.err" >&2
  exit 1
fi

echo "ok: index worker is scoped to the admitted request, not the daemon environment"
