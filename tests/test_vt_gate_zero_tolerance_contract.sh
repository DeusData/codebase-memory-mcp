#!/usr/bin/env bash
# Contract: the VT gate is an exact-set, content-bound, minimum-engine,
# zero-tolerance gate. Empty, partial, duplicate, unexpected or malformed
# action output fails before a release can be described as scanned.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GATE="$ROOT/scripts/ci/check-virustotal.sh"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/vt-gate-contract.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
hash_file() {
  sha256sum "$1" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$1" | awk '{print $1}'
}

for needle in defender-endpoint-verification av-endpoint-verify false-positive false_positive; do
  if grep -q "$needle" "$GATE"; then
    fail "gate references reverted/tolerance machinery '$needle'"
  fi
done

mkdir -p "$FIX/bin" "$FIX/work/binaries/objects" "$FIX/responses"
printf 'release-bytes\n' > "$FIX/work/binaries/objects/probe"
printf 'second-object\n' > "$FIX/work/binaries/objects/probe2"
PROBE_SHA="$(hash_file "$FIX/work/binaries/objects/probe")"
PROBE2_SHA="$(hash_file "$FIX/work/binaries/objects/probe2")"
PROBE_SIZE="$(wc -c < "$FIX/work/binaries/objects/probe" | tr -d ' ')"
PROBE2_SIZE="$(wc -c < "$FIX/work/binaries/objects/probe2" | tr -d ' ')"

write_manifest() { # one|two
  local count=1 associations=3
  if [ "$1" = "two" ]; then count=2; associations=4; fi
  {
    echo '# cbm-release-scan-set-v1'
    echo "# scan_objects=$count"
    echo "# associations=$associations"
    printf 'scan_path\tsha256\tsize\tassociation_count\tassociation_kinds\n'
    printf 'objects/probe\t%s\t%s\t3\tarchive,binary,runtime\n' "$PROBE_SHA" "$PROBE_SIZE"
    if [ "$1" = "two" ]; then
      printf 'objects/probe2\t%s\t%s\t1\tarchive\n' "$PROBE2_SHA" "$PROBE2_SIZE"
    fi
  } > "$FIX/work/binaries/scan-set.tsv"
  {
    echo '# cbm-release-scan-associations-v2'
    echo "# associations=$associations"
    echo "# scan_objects=$count"
    printf 'association_type\tarchive\tarchive_sha256\tvariant\tkind\tmember\tasset_path\tmime\tscan_path\tobject_sha256\tsize\n'
    printf 'archive\tprobe.tar.gz\t%s\tstandard\tarchive\t\t\t\tobjects/probe\t%s\t%s\n' "$PROBE_SHA" "$PROBE_SHA" "$PROBE_SIZE"
    printf 'member\tprobe.tar.gz\t%s\tstandard\tbinary\tcodebase-memory-mcp\t\t\tobjects/probe\t%s\t%s\n' "$PROBE_SHA" "$PROBE_SHA" "$PROBE_SIZE"
    printf 'member\tprobe.tar.gz\t%s\tstandard\truntime\tLICENSE\t\t\tobjects/probe\t%s\t%s\n' "$PROBE_SHA" "$PROBE_SHA" "$PROBE_SIZE"
    if [ "$1" = "two" ]; then
      printf 'archive\tprobe2.zip\t%s\tstandard\tarchive\t\t\t\tobjects/probe2\t%s\t%s\n' "$PROBE2_SHA" "$PROBE2_SHA" "$PROBE2_SIZE"
    fi
  } > "$FIX/work/binaries/associations.tsv"
}
write_manifest one

# curl stub: serve canned analysis JSON selected by the ID in the API URL.
cat > "$FIX/bin/curl" <<'EOF'
#!/usr/bin/env bash
url="${@: -1}"
id="${url##*/analyses/}"
response="$STUB_RESPONSES/$id.json"
[ -f "$response" ] || exit 22
cat "$response"
EOF
chmod +x "$FIX/bin/curl"

write_response() { # id malicious suspicious undetected sha size
  cat > "$FIX/responses/$1.json" <<EOF
{"meta":{"file_info":{"sha256":"$5","size":$6}},
 "data":{"id":"$1","type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":$2,"suspicious":$3,"undetected":$4,"harmless":0,
          "timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"undetected","result":null,
               "engine_version":"1.26070","engine_update":"20260808"}}}}}
EOF
}
write_response clean-analysis 0 0 60 "$PROBE_SHA" "$PROBE_SIZE"
write_response clean-analysis-2 0 0 60 "$PROBE2_SHA" "$PROBE2_SIZE"
write_response one-malicious 1 0 59 "$PROBE_SHA" "$PROBE_SIZE"
write_response one-suspicious 0 1 59 "$PROBE_SHA" "$PROBE_SIZE"
write_response low-engines 0 0 49 "$PROBE_SHA" "$PROBE_SIZE"
write_response wrong-hash 0 0 60 "$PROBE2_SHA" "$PROBE_SIZE"
write_response wrong-size 0 0 60 "$PROBE_SHA" "$((PROBE_SIZE + 1))"

cat > "$FIX/responses/named-engine.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"named-engine","type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":1,"suspicious":0,"undetected":59,"harmless":0,
          "timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{
   "Microsoft":{"category":"malicious","result":"Trojan:Script/Wacatac.B!ml",
                "engine_version":"1.26070","engine_update":"20260808"},
   "Kaspersky":{"category":"undetected","result":null,
                "engine_version":"22","engine_update":"20260808"}
 }}}}
EOF

cat > "$FIX/responses/malformed-stats.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"malformed-stats","attributes":{"status":"completed",
 "stats":{"malicious":"0","suspicious":0,"undetected":60,"harmless":0}}}}
EOF

cat > "$FIX/responses/missing-analysis-id.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":0,"suspicious":0,"undetected":60,"harmless":0,
          "timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"undetected","result":null,
               "engine_version":"1.26070","engine_update":"20260808"}}}}}
EOF

cat > "$FIX/responses/missing-microsoft.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"missing-microsoft","type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":0,"suspicious":0,"undetected":60,"harmless":0,
          "timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Kaspersky":{"category":"undetected","result":null,
               "engine_version":"22","engine_update":"20260808"}}}}}
EOF

cat > "$FIX/responses/microsoft-timeout.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"microsoft-timeout","type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":0,"suspicious":0,"undetected":59,"harmless":0,"timeout":1,
          "confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"timeout","result":null,
               "engine_version":"1.26070","engine_update":"20260808"}}}}}
EOF

cat > "$FIX/responses/wrong-response-type.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"wrong-response-type","type":"file","attributes":{"status":"completed",
 "stats":{"malicious":0,"suspicious":0,"undetected":60,"harmless":0,
          "timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"undetected","result":null,
               "engine_version":"1.26070","engine_update":"20260808"}}}}}
EOF

cat > "$FIX/responses/missing-malicious-stat.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"missing-malicious-stat","type":"analysis","attributes":{"status":"completed",
 "stats":{"suspicious":0,"undetected":60,"harmless":0,"timeout":0,
          "confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"undetected","result":null,
               "engine_version":"1.26070","engine_update":"20260808"}}}}}
EOF

cat > "$FIX/responses/detail-aggregate-mismatch.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"detail-aggregate-mismatch","type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":0,"suspicious":0,"undetected":60,"harmless":0,
          "timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"undetected","result":null,
               "engine_version":"1.26070","engine_update":"20260808"},
            "ContradictoryEngine":{"category":"malicious","result":"Example.Test",
               "engine_version":"1","engine_update":"20260808"}}}}}
EOF

cat > "$FIX/responses/malformed-microsoft-evidence.json" <<EOF
{"meta":{"file_info":{"sha256":"$PROBE_SHA","size":$PROBE_SIZE}},
 "data":{"id":"malformed-microsoft-evidence","type":"analysis","attributes":{"status":"completed",
 "stats":{"malicious":0,"suspicious":0,"undetected":60,"harmless":0,"timeout":0,
          "confirmed-timeout":0,"failure":0,"type-unsupported":0},
 "results":{"Microsoft":{"category":"undetected","result":null,
               "engine_version":{},"engine_update":"20260808"}}}}}
EOF

url_for() { printf 'https://www.virustotal.com/gui/file-analysis/%s/detection' "$1"; }

run_gate() { # action-output -> prints rc
  rm -f "$FIX/work/binaries/vt-results.tsv" "$FIX/last.log"
  local rc=0
  (cd "$FIX/work" &&
    PATH="$FIX/bin:$PATH" \
      STUB_RESPONSES="$FIX/responses" \
      VT_API_KEY=stub \
      VT_ANALYSIS="$1" \
      VT_EXPECTED_SCAN_SET=binaries/scan-set.tsv \
      VT_ASSOCIATIONS=binaries/associations.tsv \
      VT_RESULTS_PATH=binaries/vt-results.tsv \
      VT_REQUEST_INTERVAL_SECONDS=0 \
      VT_POLL_TIMEOUT_SECONDS=2 \
      VT_CURL_TIMEOUT_SECONDS=1 \
      MIN_ENGINES=50 \
      bash "$GATE" > "$FIX/last.log" 2>&1) || rc=$?
  echo "$rc"
}

clean_output="binaries/objects/probe=$(url_for clean-analysis)"
[ "$(run_gate "$clean_output")" = "0" ] || fail "clean exact scan must pass: $(cat "$FIX/last.log")"
RESULTS="$FIX/work/binaries/vt-results.tsv"
[ -f "$RESULTS" ] || fail "clean gate did not atomically publish its results manifest"
grep -q '^# cbm-virustotal-results-v1$' "$RESULTS" || fail "results marker missing"
grep -q "objects/probe.*$PROBE_SHA.*$PROBE_SIZE.*60" "$RESULTS" || \
  fail "results do not bind path/hash/size/actual engine count"

for id in one-malicious one-suspicious; do
  [ "$(run_gate "binaries/objects/probe=$(url_for "$id")")" != "0" ] || \
    fail "$id must block"
done
[ "$(run_gate "binaries/objects/probe=$(url_for low-engines)")" != "0" ] || \
  fail "completed analysis below the 50-engine policy must block"
grep -q 'only 49/49 decisive engines' "$FIX/last.log" || fail "low-engine failure is not explicit"

[ "$(run_gate "binaries/objects/probe=$(url_for named-engine)")" != "0" ] || \
  fail "named-engine detection must block"
grep -q 'Microsoft = Trojan:Script/Wacatac.B!ml' "$FIX/last.log" || \
  fail "gate must report the flagging engine and label"
grep -q $'objects/probe\t.*\t1\t0\t' "$FIX/work/binaries/vt-results.tsv" || \
  fail "blocked completed-set counts were not preserved as workflow evidence"
if grep -q 'detected by: Kaspersky' "$FIX/last.log"; then
  fail "gate listed a non-flagging engine as a detection"
fi

for id in wrong-hash wrong-size malformed-stats missing-analysis-id missing-microsoft microsoft-timeout wrong-response-type missing-malicious-stat detail-aggregate-mismatch malformed-microsoft-evidence; do
  [ "$(run_gate "binaries/objects/probe=$(url_for "$id")")" != "0" ] || \
    fail "$id response must fail closed"
done


# The independently validated association rows, not a mutable summary cell,
# decide whether an object is executable and therefore requires Microsoft.
write_manifest one
sed -i.bak 's/archive,binary,runtime/archive,runtime/' "$FIX/work/binaries/scan-set.tsv"
rm -f "$FIX/work/binaries/scan-set.tsv.bak"
[ "$(run_gate "$clean_output")" != "0" ] || \
  fail "scan-set kinds that contradict association rows must block"
grep -q 'association kinds differ' "$FIX/last.log" || fail "kind mismatch is not diagnosable"
write_manifest one

# Exact TSV schemas reject surplus cells instead of allowing DictReader to
# hide them under its implicit None key.
printf '\textra-cell\n' >> "$FIX/work/binaries/scan-set.tsv"
[ "$(run_gate "$clean_output")" != "0" ] || fail "surplus scan-set cells must block"
write_manifest one

# The action output itself is part of the gate. It must be a bijection with the
# expected set before any per-analysis verdict can matter.
write_manifest two
two_output="binaries/objects/probe=$(url_for clean-analysis),binaries/objects/probe2=$(url_for clean-analysis-2)"
[ "$(run_gate "$two_output")" = "0" ] || fail "two-object exact set must pass: $(cat "$FIX/last.log")"
[ "$(run_gate "binaries/objects/probe=$(url_for clean-analysis)")" != "0" ] || \
  fail "partial action output must block"
[ "$(run_gate "$two_output,binaries/objects/probe=$(url_for one-malicious)")" != "0" ] || \
  fail "duplicate action path must block"
[ "$(run_gate "$two_output,binaries/objects/unexpected=$(url_for one-malicious)")" != "0" ] || \
  fail "unexpected action path must block"
[ "$(run_gate "binaries/objects/probe")" != "0" ] || fail "malformed action entry must block"
[ "$(run_gate "binaries/objects/probe=$(url_for clean-analysis),")" != "0" ] || \
  fail "empty action entry must block"
[ "$(run_gate "binaries/objects/probe=$(url_for clean-analysis),binaries/objects/probe2=$(url_for clean-analysis)")" != "0" ] || \
  fail "one analysis URL reused for two objects must block"

# Local bytes are rehashed immediately before matching action output, so a
# post-extraction mutation cannot inherit the original clean result.
write_manifest one
printf 'tampered\n' > "$FIX/work/binaries/objects/probe"
[ "$(run_gate "$clean_output")" != "0" ] || fail "tampered staged object must block before polling"
grep -q 'changed after extraction' "$FIX/last.log" || fail "tamper failure is not diagnosable"

echo 'PASS: VT gate is exact-set, content-bound, >=50-engine and zero tolerance'
