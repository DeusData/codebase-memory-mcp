#!/usr/bin/env bash
# #2683: a hook client must not hash its own executable. It adopts the active
# cohort's build and the daemon proves its image (HELLO_UNPROVEN), so on CPUs
# without SHA instructions the hook no longer spends ~2 s hashing ~300 MB.
# Asserted on the work count the hook reports (bytes of executable image it
# hashed), never on wall time. Requires a TEST_SEAMS=1 binary (scripts/test.sh
# step 5 builds one): CBM_TEST_HOOK_IMAGE_WORK_FILE receives the count.
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
# PRECONDITION, not a skip: without the seam no count is ever written.
if ! LC_ALL=C grep -a -q -F 'CBM_TEST_HOOK_IMAGE_WORK_FILE' "${BINARY}"; then
  echo "binary lacks the hook image-work seam: ${BINARY}" >&2
  echo "  rebuild it with:  make -f Makefile.cbm cbm TEST_SEAMS=1" >&2
  exit 2
fi

# shellcheck source=../scripts/test-runtime.sh
source "${ROOT}/scripts/test-runtime.sh"
cbm_test_runtime_init
tmpdir="${CBM_TEST_RUNTIME_ROOT}"
cleanup() {
  CBM_CACHE_DIR="${tmpdir}/cache" "${BINARY}" daemon stop >/dev/null 2>&1 || true
  cbm_test_runtime_cleanup "${BINARY}"
}
trap cleanup EXIT

if ! CBM_CACHE_DIR="${tmpdir}/cache" "${BINARY}" daemon start >"${tmpdir}/daemon-start.log" 2>&1; then
  echo "daemon start failed on this host" >&2
  cat "${tmpdir}/daemon-start.log" >&2
  exit 2
fi

payload='{"session_id":"probe","hook_event_name":"PreToolUse","tool_name":"Grep","tool_input":{"pattern":"x","path":"/tmp"},"cwd":"/tmp"}'
work="${tmpdir}/hook-image-work"
# The daemon joins its cohort asynchronously after `daemon start` returns:
# wait for the state the assertion needs (an admitted hook), bounded.
report=""
for _attempt in $(seq 1 40); do
  rm -f "${work}"
  set +e
  printf '%s' "${payload}" | CBM_CACHE_DIR="${tmpdir}/cache" \
    CBM_TEST_HOOK_IMAGE_WORK_FILE="${work}" "${BINARY}" hook-augment \
    >"${tmpdir}/probe.out" 2>"${tmpdir}/probe.err"
  rc=$?
  set -e
  if [[ ${rc} -ne 0 ]]; then
    echo "hook-augment must fail open (exit 0); got ${rc}" >&2
    cat "${tmpdir}/probe.err" >&2
    exit 1
  fi
  report="$(cat "${work}" 2>/dev/null || true)"
  if [[ "${report}" == *"admitted=1"* ]]; then
    break
  fi
  sleep 0.5
done

if [[ "${report}" != *"admitted=1"* ]]; then
  echo "no hook probe was admitted by the daemon within the backstop" >&2
  echo "last report: ${report:-<none>}" >&2
  cat "${tmpdir}/probe.err" >&2
  CBM_CACHE_DIR="${tmpdir}/cache" "${BINARY}" daemon status >&2 2>&1 || true
  exit 1
fi
if [[ "${report}" != "bytes=0 admitted=1" ]]; then
  echo "an admitted hook must hash no executable image (#2683); report: ${report}" >&2
  exit 1
fi

echo "ok: an admitted hook client hashed 0 bytes of executable image"
