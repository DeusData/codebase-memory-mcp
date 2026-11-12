#!/usr/bin/env bash
# test_test_impact_selection.sh — contract of scripts/test-impact/selection.py,
# the only place a test-impact answer becomes what CI runs. `filter` narrows
# (exit 0, the lane's CBM_TEST_ONLY_FILE lines) only for a well-formed
# selected lane whose filter matches its own suite list; a skipped lane runs
# nothing (exit 11); everything else — run-all answers, unknown schemas or
# encodings, bad tokens, a filter that disagrees with the suites, an
# unreadable answer — must run everything (any other exit, no file left).
# `suites` lists what the team-artifact producer re-runs, `*` for run-all.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SEL="$ROOT/scripts/test-impact/selection.py"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

answer() { # name lane-json
    printf '{"schema":"cbm.test_impact.v0","decision":"selected","lanes":[%s]}\n' "$2" > "$TMP/$1.json"
}
filter_exit() { # name -> exit code of `filter`
    local rc=0
    python3 "$SEL" filter "$TMP/$1.json" --lane unit --out "$TMP/$1.only" 2> /dev/null || rc=$?
    echo "$rc"
}

# Narrowed: one whole suite, one suite by tests.
answer narrowed '{"lane":"unit","decision":"selected","suites":[
  {"suite":"alpha","mode":"whole","whole_reason":["STATIC"]},
  {"suite":"beta","mode":"tests","tests":[{"test":"b_one"},{"test":"b_two"}]}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\nbeta:b_one\nbeta:b_two\n"}}'
[ "$(filter_exit narrowed)" = 0 ] || fail "a well-formed selection did not narrow"
[ "$(cat "$TMP/narrowed.only")" = "$(printf 'alpha\nbeta:b_one\nbeta:b_two')" ] ||
    fail "narrowed filter lines differ: $(cat "$TMP/narrowed.only")"
[ "$(python3 "$SEL" suites "$TMP/narrowed.json")" = "$(printf 'alpha\nbeta')" ] ||
    fail "suites of a narrowed answer"

# --optional-out: the selected suites registered only conditionally.
answer conditional '{"lane":"unit","decision":"selected","suites":[
  {"suite":"alpha","mode":"whole","whole_reason":["CONDITIONAL","STATIC"]},
  {"suite":"gamma","mode":"whole","whole_reason":["STATIC"]}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\ngamma\n"}}'
rc=0
python3 "$SEL" filter "$TMP/conditional.json" --lane unit --out "$TMP/conditional.only" \
    --optional-out "$TMP/conditional.optional" 2> /dev/null || rc=$?
[ "$rc" = 0 ] || fail "a selection with a conditional suite did not narrow"
[ "$(cat "$TMP/conditional.optional")" = alpha ] ||
    fail "optional suites: $(cat "$TMP/conditional.optional")"
echo alpha > "$TMP/stale.optional"
answer stale '{"lane":"unit","decision":"run_all","suites":null,"runner_filter":null}'
rc=0
python3 "$SEL" filter "$TMP/stale.json" --lane unit --out "$TMP/stale.only" \
    --optional-out "$TMP/stale.optional" 2> /dev/null || rc=$?
[ "$rc" != 0 ] && [ ! -e "$TMP/stale.optional" ] || fail "a run-all answer left an optional-suites file"

# Skipped lane: nothing to run, an empty file.
answer skipped '{"lane":"unit","decision":"skip","suites":[],"runner_filter":null}'
[ "$(filter_exit skipped)" = 11 ] || fail "a skipped lane did not report nothing-selected"
[ -f "$TMP/skipped.only" ] && [ ! -s "$TMP/skipped.only" ] || fail "a skipped lane must leave an empty file"
[ -z "$(python3 "$SEL" suites "$TMP/skipped.json")" ] || fail "suites of a skipped lane"

# Every one of these must run everything and leave no filter file.
printf '{"schema":"cbm.test_impact.v0","decision":"run_all","lanes":[{"lane":"unit","decision":"run_all","suites":null,"runner_filter":null}]}\n' \
    > "$TMP/run_all.json"
answer lane_run_all '{"lane":"unit","decision":"run_all","suites":null,"runner_filter":null}'
printf '{"schema":"cbm.test_impact.v9","decision":"selected","lanes":[]}\n' > "$TMP/schema.json"
answer other_lane '{"lane":"integration","decision":"selected","suites":[{"suite":"alpha","mode":"whole"}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\n"}}'
answer encoding '{"lane":"unit","decision":"selected","suites":[{"suite":"alpha","mode":"whole"}],
  "runner_filter":{"encoding":"csv","value":"alpha"}}'
answer no_filter '{"lane":"unit","decision":"selected","suites":[{"suite":"alpha","mode":"whole"}],"runner_filter":null}'
answer bad_token '{"lane":"unit","decision":"selected","suites":[{"suite":"alpha","mode":"whole"}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\n--all\n"}}'
answer disagrees '{"lane":"unit","decision":"selected","suites":[{"suite":"alpha","mode":"whole"},{"suite":"beta","mode":"whole"}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\n"}}'
answer empty_tests '{"lane":"unit","decision":"selected","suites":[{"suite":"alpha","mode":"tests","tests":[]}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\n"}}'
answer skip_with_suites '{"lane":"unit","decision":"skip","suites":[{"suite":"alpha","mode":"whole"}],"runner_filter":null}'
printf '{"schema":"cbm.test_impact.v0","decision":"selected","lanes":[' > "$TMP/truncated.json"
for name in run_all lane_run_all schema other_lane encoding no_filter bad_token disagrees empty_tests \
    skip_with_suites truncated missing; do
    rc="$(filter_exit "$name")"
    case "$rc" in 0 | 11) fail "$name narrowed (exit $rc) instead of running everything" ;; esac
    [ ! -e "$TMP/$name.only" ] || fail "$name left a filter file behind"
done
# A stale filter from an earlier run is removed, never reused.
echo "alpha" > "$TMP/run_all.only"
[ "$(filter_exit run_all)" != 0 ] && [ ! -e "$TMP/run_all.only" ] || fail "a stale filter file survived"

[ "$(python3 "$SEL" suites "$TMP/run_all.json")" = "*" ] || fail "suites of a run-all answer"
[ "$(python3 "$SEL" suites "$TMP/lane_run_all.json")" = "*" ] || fail "suites of a run-all lane (suites: null)"

# --whole-suites: the portable view for the legs the coverage is not recorded
# on. A listed suite runs whole there: its per-test tokens collapse to the
# suite, and one the answer did not select is added, optional (a build without
# it drops it); the answer's own required suites stay required.
answer portable '{"lane":"unit","decision":"selected","suites":[
  {"suite":"alpha","mode":"whole","whole_reason":["CONDITIONAL","STATIC"]},
  {"suite":"beta","mode":"tests","tests":[{"test":"b_one"},{"test":"b_two"}]},
  {"suite":"delta","mode":"tests","tests":[{"test":"d_one"}]}],
  "runner_filter":{"encoding":"cbm-test-only-lines","value":"alpha\nbeta:b_one\nbeta:b_two\ndelta:d_one\n"}}'
printf 'beta\ngamma\n' > "$TMP/whole.txt"
rc=0
python3 "$SEL" filter "$TMP/portable.json" --lane unit --whole-suites "$TMP/whole.txt" \
    --out "$TMP/portable.only" --optional-out "$TMP/portable.optional" 2> /dev/null || rc=$?
[ "$rc" = 0 ] || fail "the portable view did not narrow (exit $rc)"
[ "$(cat "$TMP/portable.only")" = "$(printf 'alpha\nbeta\ndelta:d_one\ngamma')" ] ||
    fail "portable view lines: $(cat "$TMP/portable.only")"
[ "$(cat "$TMP/portable.optional")" = "$(printf 'alpha\ngamma')" ] ||
    fail "portable view optional suites: $(cat "$TMP/portable.optional")"
printf 'beta:b_one\n' > "$TMP/whole-bad.txt"
rc=0
python3 "$SEL" filter "$TMP/portable.json" --lane unit --whole-suites "$TMP/whole-bad.txt" \
    --out "$TMP/portable-bad.only" 2> /dev/null || rc=$?
case "$rc" in 0 | 11) fail "an unusable whole-suite list narrowed (exit $rc)" ;; esac
[ ! -e "$TMP/portable-bad.only" ] || fail "an unusable whole-suite list left a filter file"

# platform-suites: a suite is platform-specific when its file, at any given
# revision, holds SKIP_PLATFORM( or an OS/architecture #if, or the runner
# registers it under one. Removing the marker at HEAD does not declassify.
PS="$TMP/ps-repo"
mkdir -p "$PS/tests"
git -C "$PS" -c init.templateDir= init -q
printf '%s\n' 'int main(void) {' '    RUN_SELECTED_SUITE(plain);' '#ifndef _WIN32' \
    '    RUN_SELECTED_SUITE(posix_only);' '#endif' '#if defined(CBM_TEST_COVERAGE)' \
    '    RUN_SELECTED_SUITE(seamed);' '#endif' '}' > "$PS/tests/test_main.c"
printf '%s\n' '#ifdef __APPLE__' 'static int mac(void) { return 1; }' '#endif' \
    'SUITE(gated) {' '}' > "$PS/tests/test_gated.c"
printf '%s\n' 'TEST(t) {' '    SKIP_PLATFORM("windows only");' '}' 'SUITE(skipping) {' '}' \
    > "$PS/tests/test_skipping.c"
printf '%s\n' '#if FOO_LINUX_FEATURE' '#endif' 'void suite_plain(void) {' '}' \
    > "$PS/tests/test_plain.c"
git -C "$PS" add -A && git -C "$PS" -c user.name=t -c user.email=t@t commit -qm base
printf '%s\n' 'SUITE(gated) {' '}' > "$PS/tests/test_gated.c"
git -C "$PS" -c user.name=t -c user.email=t@t commit -qam "drop the conditional"
[ "$(python3 "$SEL" platform-suites --repo "$PS" --rev HEAD^1 --rev HEAD)" = \
    "$(printf 'gated\nposix_only\nskipping')" ] ||
    fail "platform suites: $(python3 "$SEL" platform-suites --repo "$PS" --rev HEAD^1 --rev HEAD)"
[ "$(python3 "$SEL" platform-suites --repo "$PS" --rev HEAD)" = "$(printf 'posix_only\nskipping')" ] ||
    fail "platform suites at HEAD alone"
rc=0
python3 "$SEL" platform-suites --repo "$PS" --rev no-such-rev > /dev/null 2>&1 || rc=$?
[ "$rc" = 1 ] || fail "an unknown revision must exit 1, got $rc"
echo "test-impact selection: all cases passed"
