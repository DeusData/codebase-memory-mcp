#!/usr/bin/env bash
# Contract: scripts/ci/self-index-coverage-gate.sh fails a branch only for a
# finding the branch ADDED (#963, #1972).
#
# The gate used to compare against a number checked into the repository. Main
# could move that number on its own, and every open pull request went red for
# a reason none of them caused. The gate now indexes the merge base and the
# branch with the same binary and compares the two. This test pins that.
#
# No seam is added to the production script. It calls the binary as
# "$BIN cli index_repository …" and "$BIN cli index_status …", so a fake $BIN
# that prints canned JSON exercises every check without indexing anything.
# Same idea as tests/repro/repro_script_summary.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
cleanup() {
    # Each gate run makes its own worktree and removes it. A run that dies
    # early can leave a registration behind, so prune before deleting.
    git -C "$REPO" worktree prune >/dev/null 2>&1 || true
    rm -rf "$WORKDIR"
}
REPO="$WORKDIR/repo"
trap cleanup EXIT
fail() {
    echo "FAIL: $*" >&2
    exit 1
}

FIXTURE="$WORKDIR/fixture"
mkdir -p "$FIXTURE"

# ── A repository with a base commit and a head commit ─────────────────────
mkdir -p "$REPO/scripts/ci" "$REPO/src"
cp "$ROOT/scripts/ci/self-index-coverage-gate.sh" "$REPO/scripts/ci/"
printf '# no allowlisted paths\n' > "$REPO/scripts/ci/coverage-gate-allowlist.txt"
printf '# recorded count\n7\n' > "$REPO/scripts/ci/parse-partial-baseline.txt"
# Check 3 measures a range against the file's own length, and exempts
# anything under 200 lines. These two are long enough to be measured.
for f in big allowed; do
    awk 'BEGIN { for (i = 1; i <= 300; i++) print "int line_" i ";" }' > "$REPO/src/$f.c"
done
git -C "$REPO" init -q
git -C "$REPO" config user.email "gate@test.invalid"
git -C "$REPO" config user.name "Gate Test"
git -C "$REPO" config commit.gpgsign false
# The fake binary reads this file to tell which tree it was pointed at.
printf 'base\n' > "$REPO/.side"
git -C "$REPO" add -A
git -C "$REPO" -c core.hooksPath=/dev/null commit -qm "base"
BASE_SHA="$(git -C "$REPO" rev-parse HEAD)"
printf 'head\n' > "$REPO/.side"
git -C "$REPO" add -A
git -C "$REPO" -c core.hooksPath=/dev/null commit -qm "head"

# ── The fake binary ───────────────────────────────────────────────────────
# index_repository answers with the tree's own side as the project name, so
# the index_status call that follows can be answered for the right side.
FAKE_BIN="$WORKDIR/fake-cbm"
cat > "$FAKE_BIN" <<'FAKE_EOF'
#!/usr/bin/env bash
set -euo pipefail
sub=""
repo_path=""
project=""
while [ $# -gt 0 ]; do
    case "$1" in
    index_repository | index_status) sub="$1" ;;
    --repo-path) repo_path="$2"; shift ;;
    --project) project="$2"; shift ;;
    esac
    shift
done
case "$sub" in
index_repository)
    side="$(tr -d '[:space:]' < "${repo_path}/.side")"
    printf '{"structuredContent":{"project":"%s"}}\n' "$side"
    ;;
index_status)
    cat "${GATE_FIXTURE_DIR}/${project}.json"
    ;;
*)
    echo "fake-cbm: unexpected call" >&2
    exit 1
    ;;
esac
FAKE_EOF
chmod +x "$FAKE_BIN"

# ── One gate run ──────────────────────────────────────────────────────────
# $1 case name, $2 expected outcome (pass|fail), $3 base JSON, $4 head JSON.
run_case() {
    name="$1"
    expect="$2"
    printf '%s\n' "$3" > "${FIXTURE}/base.json"
    printf '%s\n' "$4" > "${FIXTURE}/head.json"
    out="$(
        COVERAGE_GATE_BASE_SHA="$BASE_SHA" \
        GATE_FIXTURE_DIR="$FIXTURE" \
            bash "$REPO/scripts/ci/self-index-coverage-gate.sh" "$FAKE_BIN" 2>&1
    )" && rc=0 || rc=$?
    if [ "$expect" = "pass" ] && [ "$rc" -ne 0 ]; then
        printf '%s\n' "$out" >&2
        fail "${name}: expected the gate to pass, it exited ${rc}"
    fi
    if [ "$expect" = "fail" ] && [ "$rc" -eq 0 ]; then
        printf '%s\n' "$out" >&2
        fail "${name}: expected the gate to fail, it exited 0"
    fi
    LAST_OUT="$out"
    echo "ok: ${name}"
}

# ── JSON builders ─────────────────────────────────────────────────────────
# $1 partial count, $2 partial files array, $3 unusable files array,
# $4 partial truncated flag.
status() {
    cat <<EOF
{"structuredContent":{
  "parse_partial":{"count":${1},"truncated":${4:-false},"files":${2}},
  "parse_unusable":{"count":0,"truncated":false,"files":${3}}
}}
EOF
}
NO_FILES='[]'
NARROW='[{"path":"src/big.c","error_ranges":"10-20,40-50"}]'
WIDE='[{"path":"src/big.c","error_ranges":"10-200"}]'
WIDE_ALLOWED='[{"path":"src/allowed.c","error_ranges":"10-200"}]'
CLIPPED='[{"path":"src/big.c","error_ranges":"10-20,40-50+7"}]'
UNUSABLE='[{"path":"src/big.c"}]'

# ── 1. A whole-file parse failure ─────────────────────────────────────────
run_case "unusable at both sides is not this branch's doing" pass \
    "$(status 3 "$NARROW" "$UNUSABLE")" "$(status 3 "$NARROW" "$UNUSABLE")"
run_case "unusable new at head fails" fail \
    "$(status 3 "$NARROW" "$NO_FILES")" "$(status 3 "$NARROW" "$UNUSABLE")"
case "$LAST_OUT" in
*"whole-file parse failure on this branch and not at the merge base"*) ;;
*) fail "the unusable failure did not name the merge base" ;;
esac
run_case "unusable gone at head passes" pass \
    "$(status 3 "$NARROW" "$UNUSABLE")" "$(status 3 "$NARROW" "$NO_FILES")"

# ── 2. A clipped range list ───────────────────────────────────────────────
run_case "+N marker at both sides passes" pass \
    "$(status 3 "$CLIPPED" "$NO_FILES")" "$(status 3 "$CLIPPED" "$NO_FILES")"
run_case "+N marker new at head fails" fail \
    "$(status 3 "$NARROW" "$NO_FILES")" "$(status 3 "$CLIPPED" "$NO_FILES")"

# ── 3. One range covering too much of its file ────────────────────────────
run_case "a range already over the share at base passes" pass \
    "$(status 3 "$WIDE" "$NO_FILES")" "$(status 3 "$WIDE" "$NO_FILES")"
run_case "a range newly over the share fails" fail \
    "$(status 3 "$NARROW" "$NO_FILES")" "$(status 3 "$WIDE" "$NO_FILES")"
case "$LAST_OUT" in
*"within the share at the merge base"*) ;;
*) fail "the wide-range failure did not name the merge base" ;;
esac

# ── 4. The flagged-file count ─────────────────────────────────────────────
run_case "count equal to base passes" pass \
    "$(status 9 "$NARROW" "$NO_FILES")" "$(status 9 "$NARROW" "$NO_FILES")"
run_case "count below base passes" pass \
    "$(status 9 "$NARROW" "$NO_FILES")" "$(status 4 "$NARROW" "$NO_FILES")"
run_case "count above base fails" fail \
    "$(status 9 "$NARROW" "$NO_FILES")" "$(status 10 "$NARROW" "$NO_FILES")"
case "$LAST_OUT" in
*"parse_partial_count is 10 on this branch against 9 at the merge base"*) ;;
*) fail "the count failure did not print both sides" ;;
esac

# ── The recorded number reports, and never fails ──────────────────────────
# parse-partial-baseline.txt records 7. A head count far from it must still
# pass, because enforcing that number is what #1972 was.
run_case "the recorded number does not gate" pass \
    "$(status 40 "$NARROW" "$NO_FILES")" "$(status 40 "$NARROW" "$NO_FILES")"
case "$LAST_OUT" in
*"recorded=7"*) ;;
*) fail "the recorded number was not reported" ;;
esac
case "$LAST_OUT" in
*"not a failure"*) ;;
*) fail "the drift note did not say it is not a failure" ;;
esac

# ── Both sides print on every run ─────────────────────────────────────────
case "$LAST_OUT" in
*"==> base ${BASE_SHA}: parse_partial=40"*) ;;
*) fail "the base side was not printed" ;;
esac
case "$LAST_OUT" in
*"==> head:            parse_partial=40"*) ;;
*) fail "the head side was not printed" ;;
esac

# ── A clipped file list still stops the run outright ──────────────────────
# This check cannot be differential. A clipped list means the three list-based
# checks above saw only part of the tree and would print PASS anyway.
run_case "a clipped file list at head fails" fail \
    "$(status 3 "$NARROW" "$NO_FILES")" "$(status 3 "$NARROW" "$NO_FILES" true)"
case "$LAST_OUT" in
*"clipped its parse_partial file list on the head tree"*) ;;
*) fail "the clipped-list failure did not name the tree" ;;
esac

# ── The allowlist skips a path on both sides ──────────────────────────────
printf '# a written reason belongs above each path\nsrc/allowed.c\n' \
    > "$REPO/scripts/ci/coverage-gate-allowlist.txt"
git -C "$REPO" add -A
git -C "$REPO" -c core.hooksPath=/dev/null commit -qm "allowlist src/allowed.c"
run_case "an allowlisted path newly over the share passes" pass \
    "$(status 3 "$NARROW" "$NO_FILES")" "$(status 3 "$WIDE_ALLOWED" "$NO_FILES")"

# ── The base commit has to be resolvable ──────────────────────────────────
printf '%s\n' "$(status 3 "$NARROW" "$NO_FILES")" > "${FIXTURE}/base.json"
printf '%s\n' "$(status 3 "$NARROW" "$NO_FILES")" > "${FIXTURE}/head.json"
out="$(
    COVERAGE_GATE_BASE_SHA="0000000000000000000000000000000000000000" \
    GATE_FIXTURE_DIR="$FIXTURE" \
        bash "$REPO/scripts/ci/self-index-coverage-gate.sh" "$FAKE_BIN" 2>&1
)" && rc=0 || rc=$?
[ "$rc" -ne 0 ] || fail "an unreachable base commit must not pass"
case "$out" in
*"base commit"*) ;;
*) fail "an unreachable base commit did not say so: $out" ;;
esac
echo "ok: an unreachable base commit stops the run"

# ── The interface contract ────────────────────────────────────────────────
out="$(bash "$REPO/scripts/ci/self-index-coverage-gate.sh" --help 2>&1)" || fail "--help exited non-zero"
case "$out" in
*"Usage:"*) ;;
*) fail "--help printed no Usage: block" ;;
esac
case "$out" in
*"merge base"*) ;;
*) fail "--help does not describe the merge-base comparison" ;;
esac
echo "ok: --help describes the merge-base comparison"

echo "PASS: coverage-gate contract"
