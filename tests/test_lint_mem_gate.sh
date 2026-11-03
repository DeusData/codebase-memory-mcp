#!/usr/bin/env bash
# Regression contract for clang-tidy's `,-warnings-as-errors` diagnostic suffix
# (#2442). With WarningsAsErrors enabled, clang-tidy prints an analyzer check as
# [clang-analyzer-<check>,-warnings-as-errors]; a gate that only accepts
# [<check>] drops every such line and reports a clean gate. Both directions are
# pinned, and every assertion is reported, so a regression names what broke:
#   1. a suffixed finding WITH an argued whitelist entry is counted under its
#      plain check name and suppressed (exit 0, "1 argued false positive");
#   2. a suffixed finding WITHOUT an entry fails the gate (exit 1) and is
#      reported under its plain check name. This is the negative assertion: a
#      parser that drops the suffixed line reports clean (exit 0) here.
# Reverting the suffix group in FINDING_RE (scripts/lint-mem-gate.py) turns
# both red.
#
# The gate runs from inside the fixture directory with RELATIVE paths. On the
# Windows leg python3 is a native MSYS2 build: MSYS converts path ARGUMENTS for
# it, but never the diagnostic text on stdin, so an absolute /c/... path in a
# finding would not resolve there, and a C:/... one would not parse (the file
# field ends at the first colon).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GATE="$ROOT/scripts/lint-mem-gate.py"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/lint-mem-gate.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

failures=0
fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

show() {
    sed 's/^/    | /' "$1" >&2
}

# A subshell, so the test's own shell never sits inside $FIX (Windows cannot
# remove a directory that is a process's working directory).
run_gate() {
    (cd "$FIX" && LINT_MEM_WHITELIST=whitelist.txt python3 "$GATE" "$@")
}

cat > "$FIX/sample.c" <<'EOF'
int sample(int *value) {
    return *value;
}

int unlisted(int *value) {
    return *value;
}
EOF

# A native Windows python3 writes CRLF; strip it before the hash is reused.
HASH="$(run_gate --hash sample.c sample | tr -d '\r' | awk '/^segment-sha256:/ {print $2}')"
if [[ ! "$HASH" =~ ^[0-9a-f]{64}$ ]]; then
    echo "FAIL: lint-mem-gate.py --hash sample.c sample printed no segment hash (got '$HASH')" >&2
    exit 1
fi

cat > "$FIX/whitelist.txt" <<EOF
## sample.c :: sample :: clang-analyzer-core.NullDereference
segment-sha256: $HASH
why: |
  The test deliberately models a diagnostic that is suppressed by the fixture whitelist.
  This text is long enough to exercise the gate's normal argument validation path.
tried: |
  The fixture is intentionally minimal and only verifies diagnostic parsing.
EOF

SUFFIX='[clang-analyzer-core.NullDereference,-warnings-as-errors]'

# 1. Whitelisted: counted under the plain check name, then suppressed.
rc=0
printf '%s\n' "sample.c:2:12: warning: possible null dereference $SUFFIX" \
    | run_gate > /dev/null 2> "$FIX/whitelisted.err" || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "a whitelisted suffixed finding left the gate at exit $rc, expected 0"
    show "$FIX/whitelisted.err"
fi
if ! grep -q 'memory gate clean (1 argued false positive' "$FIX/whitelisted.err"; then
    fail "a whitelisted suffixed finding was not counted as 1 argued false positive"
    show "$FIX/whitelisted.err"
fi

# 2. Not whitelisted: the gate must fail and name the finding.
rc=0
printf '%s\n' "sample.c:6:12: warning: possible null dereference $SUFFIX" \
    | run_gate > /dev/null 2> "$FIX/unlisted.err" || rc=$?
if [ "$rc" -ne 1 ]; then
    fail "an unwhitelisted suffixed finding left the gate at exit $rc, expected 1" \
        "(exit 0 = the suffixed line was parsed as no finding, #2442)"
    show "$FIX/unlisted.err"
fi
if ! grep -qF 'sample.c:6: possible null dereference [clang-analyzer-core.NullDereference]' "$FIX/unlisted.err" ||
    ! grep -qF 'no whitelist entry for unlisted()' "$FIX/unlisted.err" ||
    ! grep -qF '1 finding(s) unaccounted for' "$FIX/unlisted.err"; then
    fail "the gate did not report the unwhitelisted suffixed finding under its plain check name"
    show "$FIX/unlisted.err"
fi

if [ "$failures" -ne 0 ]; then
    echo "lint-mem-gate warning suffix contract: $failures assertion(s) failed" >&2
    exit 1
fi
echo 'lint-mem-gate warning suffix contract passed (whitelisted + unwhitelisted suffixed findings)'
