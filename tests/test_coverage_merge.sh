#!/usr/bin/env bash
# test_coverage_merge.sh — contract of scripts/test-impact/coverage-merge.py,
# the incremental coverage map of a main-branch push: re-run suites come from
# the fresh map verbatim, every other listed suite is carried with its function
# ids remapped by (file, name) onto the new table, a function the new image no
# longer has drops out, an unlisted suite is dropped, and the metadata is
# recounted so the C reader checks it like a full map.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MERGE="$ROOT/scripts/test-impact/coverage-merge.py"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$TMP/previous" "$TMP/fresh"
# Previous image: f, g, gone. Suite a covers f+g, b covers g+gone, old is gone.
printf '0\tsrc/x.c\tf\n1\tsrc/x.c\tg\n2\tsrc/y.c\tgone\n' > "$TMP/previous/functions.tsv"
printf 'a:*\tcomplete\t\t\na:t1\tcomplete\t\t0 1\nb:*\tcomplete\t\t\nb:t2\tincomplete\tkilled child\t1 2\nold:*\tcomplete\t\t\nold:t3\tcomplete\t\t0\n' \
    > "$TMP/previous/tests.tsv"
cat > "$TMP/previous/meta.json" <<'EOF'
{"format": 1, "commit": "1111111111111111111111111111111111111111", "platform": "p",
 "llvm_profdata": "v", "functions": 3, "functions_compiled": 3, "tests": 3, "incomplete": 1,
 "suites": [{"suite": "a", "tests": 1, "incomplete": 0, "exit": 0, "wall_s": 1.0},
            {"suite": "b", "tests": 1, "incomplete": 1, "exit": 0, "wall_s": 2.0},
            {"suite": "old", "tests": 1, "incomplete": 0, "exit": 0, "wall_s": 3.0}]}
EOF
# New image: ids shifted, `gone` removed, `h` new. Only suite a re-ran.
printf '0\tsrc/x.c\th\n1\tsrc/x.c\tf\n2\tsrc/x.c\tg\n' > "$TMP/fresh/functions.tsv"
printf 'a:*\tcomplete\t\t\na:t1\tcomplete\t\t0 1\n' > "$TMP/fresh/tests.tsv"
cat > "$TMP/fresh/meta.json" <<'EOF'
{"format": 1, "commit": "2222222222222222222222222222222222222222", "platform": "p",
 "llvm_profdata": "v", "functions": 3, "functions_compiled": 3, "tests": 1, "incomplete": 0,
 "suites": [{"suite": "a", "tests": 1, "incomplete": 0, "exit": 0, "wall_s": 1.5}]}
EOF
printf 'a\nb\n' > "$TMP/listed.txt"

python3 "$MERGE" --previous "$TMP/previous" --fresh "$TMP/fresh" --suites "$TMP/listed.txt" \
    --commit 3333333333333333333333333333333333333333 --out "$TMP/out" > /dev/null

# Byte-exact, trailing newlines included (no cmp: diffutils is not on the
# Windows legs).
[ "$(cat "$TMP/out/functions.tsv"; echo .)" = "$(cat "$TMP/fresh/functions.tsv"; echo .)" ] ||
    fail "the function table is not the new image's"
expected="$(printf 'a:*\tcomplete\t\t\na:t1\tcomplete\t\t0 1\nb:*\tcomplete\t\t\nb:t2\tincomplete\tkilled child\t2\n')"
[ "$(cat "$TMP/out/tests.tsv")" = "$expected" ] || fail "merged rows: $(cat "$TMP/out/tests.tsv")"
python3 - "$TMP/out/meta.json" <<'EOF' || fail "merged metadata"
import json, sys
meta = json.load(open(sys.argv[1]))
assert meta["commit"] == "3" * 40, meta["commit"]
assert (meta["tests"], meta["incomplete"]) == (2, 1), (meta["tests"], meta["incomplete"])
suites = {s["suite"]: s for s in meta["suites"]}
assert sorted(suites) == ["a", "b"], sorted(suites)
assert (suites["a"]["tests"], suites["a"]["wall_s"]) == (1, 1.5), suites["a"]
assert (suites["b"]["tests"], suites["b"]["incomplete"]) == (1, 1), suites["b"]
EOF

# A re-run suite the new runner does not list is an inconsistent input.
printf 'b\n' > "$TMP/listed-wrong.txt"
if python3 "$MERGE" --previous "$TMP/previous" --fresh "$TMP/fresh" --suites "$TMP/listed-wrong.txt" \
    --commit 3333333333333333333333333333333333333333 --out "$TMP/out2" > /dev/null 2>&1; then
    fail "a re-run suite missing from the runner's list was accepted"
fi
echo "coverage merge: all cases passed"
