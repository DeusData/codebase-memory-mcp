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
echo "test-impact selection: all cases passed"
