#!/usr/bin/env bash
# PR CI's test selection in the parallel harness (scripts/run-tests-parallel.sh,
# CBM_TEST_SELECTION_FILE / CBM_TEST_SELECTION_OPTIONAL), driven over the
# runner just built — the same binary and scheduler every leg runs:
#   1. only the selected suites are launched, each runner applies the tokens
#      (a narrowed suite deselects its other tests), and the shard manifest
#      records the selected list and the selection's digest;
#   2. a selected suite this build does not register FAILS the run, unless the
#      selection marks it conditional, when it is dropped with a notice;
#   3. a malformed selection and a selected test the build does not have FAIL.
# A selection that quietly ran less than it named would report green for work
# that never ran.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="${CBM_TEST_RUNNER:-${ROOT}/build/c/test-runner}"
if [[ ! -x "${RUNNER}" && -x "${RUNNER}.exe" ]]; then
    RUNNER="${RUNNER}.exe"
fi
if [[ ! -x "${RUNNER}" ]]; then
    echo "missing test runner: ${RUNNER}" >&2
    exit 2
fi
# Relative paths, as the real run uses them: the native Windows runner reads
# the selection file the harness writes under its log directory.
cd "${ROOT}"
RUNNER_REL="${RUNNER#"${ROOT}"/}"
LOGS="$(dirname "${RUNNER_REL}")/test-logs-selection"

# Anything inherited from the caller would decide the cases below.
unset CBM_TEST_ONLY CBM_TEST_ONLY_FILE CBM_TEST_SELECTION_FILE CBM_TEST_SELECTION_OPTIONAL
unset CBM_TEST_SHARD CBM_TEST_LEG

NAMED_SUITE=dyn_array
OTHER_SUITE=str_intern
OTHER_TEST="$(awk -F'[()]' '/^[[:space:]]*RUN_TEST\(/ { print $2; exit }' "tests/test_${OTHER_SUITE}.c")"
OTHER_TOTAL="$(awk '/^[[:space:]]*RUN_TEST\(/ { n++ } END { print n + 0 }' "tests/test_${OTHER_SUITE}.c")"
if [[ -z "${OTHER_TEST}" || "${OTHER_TOTAL}" -lt 2 ]]; then
    echo "fixture suite changed shape: ${OTHER_SUITE}=${OTHER_TOTAL}" >&2
    exit 2
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/cbm-selection.XXXXXX")"
trap 'rm -rf "${tmpdir}" "${LOGS}"' EXIT
FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# harness <case> <selection lines> [optional suites] -> RC, OUT
harness() {
    printf '%b' "$2" > "${tmpdir}/$1.sel"
    local optional=()
    if [[ -n "${3:-}" ]]; then
        printf '%b' "$3" > "${tmpdir}/$1.opt"
        optional=(CBM_TEST_SELECTION_OPTIONAL="${tmpdir}/$1.opt")
    fi
    RC=0
    OUT="$(env CBM_TEST_LOG_DIR="${LOGS}" CBM_TEST_SELECTION_FILE="${tmpdir}/$1.sel" \
        ${optional[@]+"${optional[@]}"} \
        bash scripts/run-tests-parallel.sh "${RUNNER_REL}" 2 2>&1 | tr -d '\r')" || RC=$?
}
ran_suites() {
    awk '{ print $1 }' "${LOGS}/results.txt" | sort | tr '\n' ' '
}

# 1. Narrowed: one whole suite, one suite by a single test.
harness narrowed "${NAMED_SUITE}\n${OTHER_SUITE}:${OTHER_TEST}\n"
[[ "${RC}" -eq 0 ]] || fail "a valid selection failed (exit ${RC}): ${OUT}"
[[ "$(ran_suites)" == "${NAMED_SUITE} ${OTHER_SUITE} " ]] ||
    fail "launched suites: $(ran_suites)"
grep -q "deselected" "${LOGS}/${OTHER_SUITE}.log" ||
    fail "${OTHER_SUITE} ran unnarrowed: $(tail -3 "${LOGS}/${OTHER_SUITE}.log")"
grep -qE '^ *1 passed' "${LOGS}/${OTHER_SUITE}.log" ||
    fail "${OTHER_SUITE} did not run exactly its selected test"
[[ "$(sed -n '/^--- slice ---$/,$p' "${LOGS}/shard-manifest.txt" | tail -n +2 | sort | tr '\n' ' ')" == \
    "${NAMED_SUITE} ${OTHER_SUITE} " ]] || fail "the manifest slice is not the selected list"
grep -qE '^selection_sha256=[0-9a-f]{64}$' "${LOGS}/shard-manifest.txt" ||
    fail "the manifest does not record the selection digest"

# 2. A selected suite this build does not register.
harness unknown_suite "${NAMED_SUITE}\nno_such_suite\n"
[[ "${RC}" -ne 0 && "${OUT}" == *"does not register"*"no_such_suite"* ]] ||
    fail "an unregistered selected suite did not fail the run (exit ${RC})"
harness optional_suite "${NAMED_SUITE}\nno_such_suite\n" "no_such_suite\n"
[[ "${RC}" -eq 0 && "${OUT}" == *"not in this build, not run: no_such_suite"* ]] ||
    fail "a conditional suite absent from this build was not dropped (exit ${RC}): ${OUT}"
[[ "$(ran_suites)" == "${NAMED_SUITE} " ]] || fail "optional case launched: $(ran_suites)"

# 3. Malformed selections and unknown tests fail.
harness malformed "${NAMED_SUITE} extra\n"
[[ "${RC}" -ne 0 ]] || fail "a malformed selection was accepted"
harness empty ""
[[ "${RC}" -ne 0 ]] || fail "an empty selection was accepted"
harness unknown_test "${OTHER_SUITE}:no_such_test_in_this_build\n"
[[ "${RC}" -ne 0 ]] || fail "a selected test the build does not have did not fail the run"

if [[ "${FAILURES}" -ne 0 ]]; then
    echo "test selection harness: ${FAILURES} case(s) failed" >&2
    exit 1
fi
echo "test selection harness: all cases passed"
