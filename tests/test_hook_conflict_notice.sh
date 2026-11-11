#!/usr/bin/env bash
# #1388: a hook client that cannot join because the active daemon runs a
# DIFFERENT build must say so on stdout (systemMessage) - stdout is the only
# hook channel Claude Code surfaces, so stderr-only reporting reads as eternal
# silence in-session. Requires a TEST_SEAMS=1 binary (scripts/test.sh step 5
# builds one): CBM_TEST_HOOK_CLIENT_BUILD forces the client fingerprint.
#
# STATUS: LOCAL-ONLY, NOT WIRED INTO scripts/test.sh (see the Step 5d comment
# there). WHY: on every CI leg this test's forced client/daemon build mismatch
# produces no cohort conflict, so the notice under test is never triggered and
# the assertion fails for a reason unrelated to the fix.
# WHAT WAS TRIED (2026-08-05, PR #1441):
#   - hardened the probe loop to wait for the asserted state with a bounded
#     backstop instead of a single post-start probe (cohort join is async);
#   - asserted the seam's presence in the binary up front, mirroring
#     tests/test_worker_watchdog.sh - the seam IS present on CI;
#   - replaced a `seq`-derived fingerprint with a length-checked literal, in
#     case `seq` was absent on a leg - it was not the cause;
#   - dumped stdout, the forced fingerprint, and `daemon status` on expiry:
#     the daemon is active on a DIFFERENT build fingerprint, yet the forced
#     client still joins without a conflict, which does not happen locally.
# The remaining suspect is the cohort admission path behaving differently under
# the CI environment; that is recorded as a follow-up rather than papered over.
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

# PRECONDITION, not a skip (same doctrine as tests/test_worker_watchdog.sh): this
# test drives the hook-client build seam, which is compiled out of ordinary
# builds. Against a seam-less binary no conflict can ever occur and the failure
# would read as "the notice is missing" instead of "the capability is absent".
if ! LC_ALL=C grep -a -q -F 'CBM_TEST_HOOK_CLIENT_BUILD' "${BINARY}"; then
  echo "binary lacks the hook-client build seam: ${BINARY}" >&2
  echo "  rebuild it with:  make -f Makefile.cbm cbm TEST_SEAMS=1" >&2
  echo "  or run this test through scripts/test.sh, which does that for you." >&2
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
  echo "daemon start failed on this host - cannot exercise the conflict path" >&2
  cat "${tmpdir}/daemon-start.log" >&2
  exit 2
fi
payload='{"session_id":"probe","hook_event_name":"PreToolUse","tool_name":"Grep","tool_input":{"pattern":"x","path":"/tmp"},"cwd":"/tmp"}'
forced_build="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
if [[ ${#forced_build} -ne 64 ]]; then
  echo "internal: forced fingerprint must be 64 hex chars, got ${#forced_build}" >&2
  exit 2
fi

# The daemon's cohort join completes asynchronously after `daemon start`
# returns. Wait for the STATE the assertion needs - a probe that observes the
# build conflict - with a bounded liveness backstop; never a bare sleep. The
# throttled notice marker is cleared before each probe so the stdout
# assertion stays valid on whichever probe first observes the conflict.
rc=0
out=""
for _attempt in $(seq 1 40); do
  rm -f "${tmpdir}/cache/.hook-daemon-absent-notice"
  set +e
  out="$(printf '%s' "${payload}" | CBM_CACHE_DIR="${tmpdir}/cache" \
    CBM_TEST_HOOK_CLIENT_BUILD="${forced_build}" \
    "${BINARY}" hook-augment 2>"${tmpdir}/probe.err")"
  rc=$?
  set -e
  if [[ ${rc} -ne 0 ]]; then
    echo "hook-augment must fail open (exit 0); got ${rc}" >&2
    cat "${tmpdir}/probe.err" >&2
    exit 1
  fi
  if grep -q "conflicting CBM process" "${tmpdir}/probe.err"; then
    break
  fi
  sleep 0.5
done

if ! grep -q "conflicting CBM process" "${tmpdir}/probe.err"; then
  echo "no probe observed the build conflict within the backstop - daemon cohort" >&2
  echo "never became active, or this is not a TEST_SEAMS=1 binary" >&2
  echo "--- daemon-start.log ---" >&2
  cat "${tmpdir}/daemon-start.log" >&2
  echo "--- last probe stderr (empty means no conflict was raised) ---" >&2
  cat "${tmpdir}/probe.err" >&2
  echo "--- last probe stdout ---" >&2
  printf '%s\n' "${out}" >&2
  echo "--- diagnosis ---" >&2
  echo "forced_build=${forced_build} (len ${#forced_build})" >&2
  echo "seam present in binary: yes (asserted above)" >&2
  CBM_CACHE_DIR="${tmpdir}/cache" "${BINARY}" daemon status >&2 2>&1 || true
  exit 2
fi
if ! printf '%s' "${out}" | grep -q "systemMessage"; then
  echo "conflicted daemon must surface a stdout systemMessage to the hook caller (#1388)" >&2
  echo "stdout was: ${out}" >&2
  exit 1
fi
if ! printf '%s' "${out}" | grep -q "daemon stop"; then
  echo "the systemMessage must carry the daemon-restart guidance (#1388)" >&2
  echo "stdout was: ${out}" >&2
  exit 1
fi

echo "ok: daemon build conflicts reach the hook caller as a systemMessage"
