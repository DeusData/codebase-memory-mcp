#!/usr/bin/env bash
# Contract: the VirusTotal release gate (scripts/ci/check-virustotal.sh) is
# ZERO tolerance — ANY detection on ANY artifact blocks, with no version
# carve-out, no engine carve-out, and no evidence side-channel.
#
# A tolerance path existed briefly (single-engine Microsoft "!ml" on
# pre-releases, endpoint-evidence-gated) and was deliberately reverted: the
# project does not ship binaries carrying a VirusTotal detection, full stop.
# False positives are resolved upstream (Microsoft FP submission for the exact
# hashes, then re-run the verify job — no rebuild), never by loosening the
# gate. This contract pins that decision against a stubbed VT API so a future
# "just tolerate this one heuristic" change has to consciously delete it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GATE="$ROOT/scripts/ci/check-virustotal.sh"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/vt-gate-contract.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# Tripwire for the specific reverted mechanism (behavioral checks below are
# the real contract; these names only catch that exact machinery returning).
for needle in defender-endpoint-verification av-endpoint-verify; do
  if grep -q "$needle" "$GATE"; then
    fail "gate references '$needle' — the reverted evidence side-channel is back"
  fi
done

mkdir -p "$FIX/bin" "$FIX/work/binaries" "$FIX/responses"

# curl stub: serve the canned VT analysis JSON selected by the id in the URL.
cat > "$FIX/bin/curl" <<'EOF'
#!/usr/bin/env bash
url="${@: -1}"
id="${url##*/analyses/}"
resp="$STUB_RESPONSES/$id.json"
[ -f "$resp" ] || exit 22
cat "$resp"
EOF
chmod +x "$FIX/bin/curl"

mk_response() { # id malicious suspicious
  cat > "$FIX/responses/$1.json" <<EOF
{"data":{"attributes":{"status":"completed",
  "stats":{"malicious":$2,"suspicious":$3,"undetected":60,"harmless":0}}}}
EOF
}
mk_response clean 0 0
mk_response one_malicious 1 0
mk_response one_suspicious 0 1

# Same detection as one_malicious, but carrying the per-engine results a real
# analysis returns. Pins that the gate NAMES the engine that fired.
cat > "$FIX/responses/named_engine.json" <<'EOF'
{"data":{"attributes":{"status":"completed",
  "stats":{"malicious":1,"suspicious":0,"undetected":60,"harmless":0},
  "results":{
    "Microsoft":{"category":"malicious","result":"Trojan:Win32/Wacatac.B!ml",
                 "engine_version":"1.1.24010.10","engine_update":"20260807"},
    "Kaspersky":{"category":"undetected","result":null,
                 "engine_version":"22.0.1.28","engine_update":"20260807"}
  }}}}
EOF

echo "release-bytes" > "$FIX/work/binaries/probe"

run_gate() { # version analysis-id -> echoes exit code
  rm -f /tmp/vt_gate_fail
  local rc=0
  (cd "$FIX/work" &&
    PATH="$FIX/bin:$PATH" \
      STUB_RESPONSES="$FIX/responses" \
      VT_API_KEY="stub" \
      VERSION="$1" \
      VT_ANALYSIS="binaries/probe=https://www.virustotal.com/gui/file-analysis/$2/detection" \
      bash "$GATE" >"$FIX/last.log" 2>&1) || rc=$?
  echo "$rc"
}

[ "$(run_gate v0.9.1 clean)" = "0" ] || fail "clean scan must pass"

for version in v0.9.1 v0.9.1-rc.1 v0.9.1-pre1 v1.0.0-alpha.1; do
  [ "$(run_gate "$version" one_malicious)" != "0" ] || \
    fail "1 malicious must block on $version (pre-release carve-outs are gone)"
  [ "$(run_gate "$version" one_suspicious)" != "0" ] || \
    fail "1 suspicious must block on $version"
done

# A detection must NAME the engine that fired.
#
# The gate reported counts only ("1 malicious / 59 engines"), which is the one
# thing a reader cannot act on: a false-positive submission goes to a SPECIFIC
# vendor (and the WDSI developer path is a web form, so there is no API to guess
# with), and triage starts with "known-noisy engine, or a new family?". In the
# 2026-08-07 dry run three artifacts flagged and the logs could not say by what,
# so the answer had to be fetched by hand from the web UI afterwards.
[ "$(run_gate v0.9.1 named_engine)" != "0" ] || fail "named-engine detection must still block"
grep -q "Microsoft" "$FIX/last.log" || \
  fail "gate must name the flagging engine (Microsoft) — got: $(cat "$FIX/last.log")"
grep -q "Trojan:Win32/Wacatac.B!ml" "$FIX/last.log" || \
  fail "gate must report the detection label, not just the vendor"
# Engines that did NOT flag must not be listed, or the actionable line drowns.
if grep -q "detected by: Kaspersky" "$FIX/last.log"; then
  fail "gate listed a non-flagging engine (Kaspersky) as a detection"
fi

# Naming engines must not weaken the gate: an analysis WITHOUT per-engine
# results still blocks on the counts alone.
[ "$(run_gate v0.9.1 one_malicious)" != "0" ] || \
  fail "detection without engine detail must still block"

rm -f /tmp/vt_gate_fail
echo "PASS: VT gate is zero tolerance and names the engine that fired"
