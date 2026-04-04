#!/usr/bin/env bash
# Wait for VirusTotal scans to complete and check results.
# Expects: VT_API_KEY, VT_ANALYSIS (comma-separated "file=URL" pairs)
set -euo pipefail

MIN_ENGINES=60
rm -f /tmp/vt_gate_fail

echo "=== Waiting for VirusTotal scans to fully complete ==="

echo "$VT_ANALYSIS" | tr ',' '\n' | while IFS= read -r entry; do
  [ -z "$entry" ] && continue
  FILE=$(echo "$entry" | cut -d'=' -f1)
  URL=$(echo "$entry" | cut -d'=' -f2-)
  BASENAME=$(basename "$FILE")

  # Extract base64 analysis ID from URL
  ANALYSIS_ID=$(echo "$URL" | sed -n 's|.*/file-analysis/\([^/]*\)/.*|\1|p')
  if [ -z "$ANALYSIS_ID" ]; then
    ANALYSIS_ID=$(echo "$URL" | grep -oE '[a-f0-9]{64}')
    if [ -z "$ANALYSIS_ID" ]; then
      echo "BLOCKED: Cannot parse VirusTotal URL: $URL"
      echo "FAIL" >> /tmp/vt_gate_fail
      continue
    fi
  fi

  # Poll until completed (max 120 min)
  SCAN_COMPLETE=false
  for attempt in $(seq 1 720); do
    RESULT=$(curl -sf --max-time 10 \
      -H "x-apikey: $VT_API_KEY" \
      "https://www.virustotal.com/api/v3/analyses/$ANALYSIS_ID" 2>/dev/null || echo "")

    if [ -z "$RESULT" ]; then
      echo "  $BASENAME: waiting (attempt $attempt)..."
      sleep 10
      continue
    fi

    STATS=$(echo "$RESULT" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read())
attrs = d.get('data', {}).get('attributes', {})
status = attrs.get('status', 'queued')
stats = attrs.get('stats', {})
malicious = stats.get('malicious', 0)
suspicious = stats.get('suspicious', 0)
undetected = stats.get('undetected', 0)
harmless = stats.get('harmless', 0)
total = sum(stats.values())
completed = malicious + suspicious + undetected + harmless
print(f'{status},{malicious},{suspicious},{completed},{total}')
" 2>/dev/null || echo "queued,0,0,0,0")

    STATUS=$(echo "$STATS" | cut -d',' -f1)
    MALICIOUS=$(echo "$STATS" | cut -d',' -f2)
    SUSPICIOUS=$(echo "$STATS" | cut -d',' -f3)
    COMPLETED=$(echo "$STATS" | cut -d',' -f4)
    TOTAL=$(echo "$STATS" | cut -d',' -f5)

    if [ "$STATUS" = "completed" ] && [ "$COMPLETED" -ge "$MIN_ENGINES" ]; then
      SCAN_COMPLETE=true
      if [ "$MALICIOUS" -gt 0 ] || [ "$SUSPICIOUS" -gt 0 ]; then
        echo "BLOCKED: $BASENAME flagged ($MALICIOUS malicious, $SUSPICIOUS suspicious / $COMPLETED engines)"
        echo "  $URL"
        echo "FAIL" >> /tmp/vt_gate_fail
      else
        echo "OK: $BASENAME clean ($COMPLETED engines, 0 detections)"
      fi
      break
    fi

    echo "  $BASENAME: $COMPLETED/$TOTAL engines ($STATUS, attempt $attempt)..."
    sleep 10
  done

  if [ "$SCAN_COMPLETE" != "true" ]; then
    echo "WARNING: $BASENAME scan did not complete in time"
    echo "FAIL" >> /tmp/vt_gate_fail
  fi
done

if [ -f /tmp/vt_gate_fail ]; then
  echo "BLOCKED: One or more VirusTotal checks failed"
  exit 1
fi
echo "=== All VirusTotal scans passed ==="
