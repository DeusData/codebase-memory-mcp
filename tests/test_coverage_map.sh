#!/usr/bin/env bash
# The per-test coverage map builder: scripts/test-impact/coverage-map.py.
#
# The map decides which tests of a spawn-heavy suite a change can reach, so what
# it must never do is claim that a test executed less than it did. This script
# pins the cases where it cannot know, each of which has to come out
# `incomplete` (always selected) instead of as a short function list:
#   - a forked child left its marker (its coverage is not accounted for);
#   - a profile that cannot be read;
#   - a test with child profiles but none of the runner itself;
#   - a suite that did not exit 0.
# It also pins the bookkeeping the engine relies on:
#   - a function is named by the SOURCE FILE its body stands in, taken from the
#     coverage mapping of the runner, not by the translation unit the profile
#     names: a unity build and a header's static inline would otherwise carry a
#     file the graph does not know them under;
#   - one function seen from two translation units is one row;
#   - a function the mapping does not list keeps what the profile says;
#   - ids in tests.tsv name rows of functions.tsv;
#   - what ran between tests lands on `<suite>:*`;
#   - the runner is started with a plain LLVM_PROFILE_FILE;
#   - no raw profile survives the run.
#
# Hermetic: a fake runner writes the profile files a real instrumented runner
# would, a fake llvm-profdata reads function names out of them and a fake
# llvm-cov prints a coverage mapping. Building the real instrumented runner
# takes minutes and belongs to the map job.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILDER="${ROOT}/scripts/test-impact/coverage-map.py"
PYTHON="${PYTHON:-python3}"
if ! command -v "${PYTHON}" >/dev/null 2>&1; then
    echo "missing interpreter: ${PYTHON}" >&2
    exit 2
fi

# Strict LLVM text records retain zero-only identities and full counter values.
PYTHONDONTWRITEBYTECODE=1 "${PYTHON}" "${ROOT}/tests/test_profile_text_parser.py"
# Pure single-image v2 producer.
PYTHONDONTWRITEBYTECODE=1 "${PYTHON}" "${ROOT}/tests/test_profile_map_v2.py"

if "${PYTHON}" -c 'import sys; sys.exit(sys.platform != "win32")'; then
    # WHY: the map is built only by the ubuntu-24.04 publish job
    # (.github/workflows/test-impact-artifact.yml), and this leg's runner and
    # LLVM tools are bash fakes. Tried: the leg under the native (mingw) python3
    # the Windows test legs install, on the Windows VM -- CreateProcess cannot
    # start an extensionless shebang script, the builder stops at its tool
    # check ("not found: .../bin/llvm-profdata", exit 2), and every assertion
    # after it fails for that one reason, so the run proves nothing about how
    # the publish job builds a map.
    echo "SKIP: builder functional leg needs a POSIX python (native Windows python3); profile-text contract passed"
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
mkdir -p "${WORK}/bin" "${WORK}/repo"
REPO="$(cd "${WORK}/repo" && pwd -P)"

# What the fake runner writes stands in for a raw profile: the names of the
# functions it "executed", one per line, as a profile would name them.
cat >"${WORK}/bin/runner" <<'RUNNER'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--list-suites" ]]; then
    # The listing run leaves the profile the builder reads the mapping with.
    printf 'main\n' >"${LLVM_PROFILE_FILE}"
    printf 'alpha\nbeta\n'
    exit 0
fi
if [[ "${LLVM_PROFILE_FILE:-unset}" != "/dev/null" ]]; then
    echo "runner started with LLVM_PROFILE_FILE=${LLVM_PROFILE_FILE:-unset}" >&2
    exit 9
fi
dir="${CBM_TEST_COVERAGE_DIR}/$1"
mkdir -p "${dir}"
case "$1" in
alpha)
    printf 'setup_fn\nshared_fn\n' >"${dir}/_setup.0.profraw"
    printf 'between_fn\n' >"${dir}/_setup.child.77.profraw"
    printf 'fa\nfb\nunity.c:static_fn\n' >"${dir}/t_one.parent.profraw"
    printf 'fc\nfirst.c:inline_fn\nsecond.c:inline_fn\n' >"${dir}/t_one.123.profraw"
    printf 'fb\n' >"${dir}/t_two.parent.profraw"
    : >"${dir}/t_two.456.forked"
    printf 'unlisted.c:fd\n' >"${dir}/t_three.789.profraw"
    printf 'UNREADABLE\n' >"${dir}/t_bad.parent.profraw"
    ;;
beta)
    printf 'fb\n' >"${dir}/t_late.parent.profraw"
    exit 3
    ;;
esac
RUNNER

cat >"${WORK}/bin/llvm-profdata" <<'PROFDATA'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
    echo "fake llvm-profdata"
    exit 0
fi
if [[ "${1:-}" == "merge" ]]; then
    # merge -sparse -o OUT IN
    cp "${!#}" "$4"
    exit 0
fi
file="${!#}"
if grep -q UNREADABLE "${file}"; then
    echo "error: ${file}: malformed raw profile" >&2
    exit 1
fi
echo "Counters:"
while IFS= read -r name; do
    printf '  %s:\n    Hash: 0x0\n    Counters: 1\n    Function count: 1\n' "${name}"
done <"${file}"
echo "Functions shown: 1"
PROFDATA

# The coverage mapping: where each function body stands. `unity.c:static_fn`
# was compiled through a unity file, `inline_fn` lives in a header two files
# include, and `unlisted.c:fd` is deliberately absent.
cat >"${WORK}/bin/llvm-cov" <<COV
#!/usr/bin/env bash
cat <<'JSON'
{"data": [{"functions": [
 {"name": "main", "filenames": ["${REPO}/tests/runner.c"]},
 {"name": "fa", "filenames": ["${REPO}/src/a.c"]},
 {"name": "fb", "filenames": ["${REPO}/src/b.c"]},
 {"name": "fc", "filenames": ["${REPO}/src/c.c"]},
 {"name": "unity.c:static_fn", "filenames": ["${REPO}/src/lsp/real_source.c"]},
 {"name": "first.c:inline_fn", "filenames": ["${REPO}/src/shared.h"]},
 {"name": "second.c:inline_fn", "filenames": ["${REPO}/src/shared.h"]},
 {"name": "setup_fn", "filenames": ["${REPO}/src/setup.c"]},
 {"name": "shared_fn", "filenames": ["${REPO}/src/setup.c"]},
 {"name": "between_fn", "filenames": ["${REPO}/src/setup.c"]},
 {"name": "never_run", "filenames": ["${REPO}/src/dead.c"]}
]}]}
JSON
COV
chmod +x "${WORK}/bin/runner" "${WORK}/bin/llvm-profdata" "${WORK}/bin/llvm-cov"

OUT="${WORK}/map"
STATUS=0
"${PYTHON}" "${BUILDER}" --runner "${WORK}/bin/runner" --out "${OUT}" \
    --llvm-bin "${WORK}/bin" --root "${WORK}/repo" >"${WORK}/stdout" 2>"${WORK}/stderr" || STATUS=$?

FAILED=0
fail() {
    echo "FAIL: $*" >&2
    FAILED=1
}

# A failed suite fails the build of the map, and the map is written anyway.
[[ "${STATUS}" -eq 1 ]] || fail "exit status ${STATUS}, expected 1 (suite beta exits 3)"
for name in functions.tsv tests.tsv meta.json; do
    [[ -s "${OUT}/${name}" ]] || fail "missing output: ${name}"
done
[[ ! -e "${OUT}/raw" ]] || fail "raw profiles were left behind"
if compgen -G "${OUT}/*.profraw" >/dev/null || compgen -G "${OUT}/*.profdata" >/dev/null; then
    fail "the profile of the listing run was left behind"
fi

# `<file>:<name>` of every function of a row, resolved through functions.tsv.
row_functions() {
    awk -F'\t' -v key="$1" '
        NR == FNR { name[$1] = $2 ":" $3; next }
        $1 == key { n = split($4, ids, " "); for (i = 1; i <= n; i++) print name[ids[i]] }
    ' "${OUT}/functions.tsv" "${OUT}/tests.tsv" | sort | tr '\n' ' '
}
row_field() {
    awk -F'\t' -v key="$1" -v col="$2" '$1 == key { print $col }' "${OUT}/tests.tsv"
}
expect_row() {
    local key="$1" status="$2" reason="$3" functions="$4"
    [[ "$(row_field "${key}" 2)" == "${status}" ]] ||
        fail "${key}: status '$(row_field "${key}" 2)', expected '${status}'"
    [[ "$(row_field "${key}" 3)" == *"${reason}"* ]] ||
        fail "${key}: reason '$(row_field "${key}" 3)', expected to contain '${reason}'"
    [[ "$(row_functions "${key}")" == "${functions}" ]] ||
        fail "${key}: functions '$(row_functions "${key}")', expected '${functions}'"
}

# Parent and child profiles are one test. The unity function carries its real
# source file, and the header function seen from two files is one function.
expect_row "alpha:t_one" complete "" \
    "src/a.c:fa src/b.c:fb src/c.c:fc src/lsp/real_source.c:static_fn src/shared.h:inline_fn "
[[ -z "$(row_field "alpha:t_one" 3)" ]] || fail "alpha:t_one: a complete test carries no reason"
# A marker without anything else still makes the test incomplete.
expect_row "alpha:t_two" incomplete "unaccounted forked child: 456" "src/b.c:fb "
# A function the mapping does not list keeps the name of the profile.
expect_row "alpha:t_three" incomplete "no parent profile" "unlisted.c:fd "
expect_row "alpha:t_bad" incomplete "unreadable profile: t_bad.parent.profraw" ""
# What ran between tests concerns every test of the suite.
expect_row "alpha:*" complete "" "src/setup.c:between_fn src/setup.c:setup_fn src/setup.c:shared_fn "
# A suite that did not exit 0: nothing it reported is trusted to be whole.
expect_row "beta:t_late" incomplete "suite did not exit 0" "src/b.c:fb "
expect_row "beta:*" incomplete "suite did not exit 0" ""

# One row for the header function, none for a function no test executed.
[[ "$(awk -F'\t' '$3 == "inline_fn"' "${OUT}/functions.tsv" | wc -l | tr -d ' ')" == "1" ]] ||
    fail "functions.tsv: inline_fn of src/shared.h is not exactly one row"
if awk -F'\t' '$3 == "never_run" { found = 1 } END { exit !found }' "${OUT}/functions.tsv"; then
    fail "functions.tsv: lists a function no test executed"
fi
# Ids are dense and unique.
awk -F'\t' '$1 != NR - 1 { bad = 1 } END { exit bad }' "${OUT}/functions.tsv" ||
    fail "functions.tsv: ids are not 0..n-1 in order"

grep -q '"incomplete": 4' "${OUT}/meta.json" || fail "meta.json: expected 4 incomplete tests"
grep -q '"tests": 5' "${OUT}/meta.json" || fail "meta.json: expected 5 tests"
# The mapping lists ten distinct functions, executed or not.
grep -q '"functions_compiled": 10' "${OUT}/meta.json" ||
    fail "meta.json: expected 10 compiled functions"

# A runner that is not there is a usage error, not an empty map.
if "${PYTHON}" "${BUILDER}" --runner "${WORK}/bin/absent" --out "${WORK}/none" \
    --llvm-bin "${WORK}/bin" >/dev/null 2>&1; then
    fail "a missing runner did not fail the builder"
fi
[[ ! -e "${WORK}/none/tests.tsv" ]] || fail "a missing runner still produced a map"

if [[ "${FAILED}" -ne 0 ]]; then
    echo "--- builder stdout" >&2
    cat "${WORK}/stdout" >&2
    echo "--- builder stderr" >&2
    cat "${WORK}/stderr" >&2
    exit 1
fi
echo "coverage map builder: all cases passed"
