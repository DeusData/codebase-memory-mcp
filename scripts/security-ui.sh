#!/usr/bin/env bash
set -euo pipefail

# Layer 6: Graph UI security audit.
#
# Audits:
#   A. Frontend asset scan (embedded JS/CSS/HTML or graph-ui/dist/)
#   B. HTTP server binding (must be 127.0.0.1 only)
#   C. RPC proxy scope (no system()/popen() in HTTP handler path)
#   D. CORS check (no wildcard Access-Control-Allow-Origin)
#
# Usage: scripts/security-ui.sh

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FAIL=0

# Use mktemp for all temp files (cross-platform safe)
SEC_TMPDIR=$(mktemp -d)
trap 'rm -rf "$SEC_TMPDIR"' EXIT

echo "=== Layer 6: Graph UI Security Audit ==="

# ── A. Frontend asset scan ───────────────────────────────────────

echo ""
echo "--- A. Frontend asset scan ---"

# Check both built dist and embedded asset source
UI_DIRS=()
[[ -d "$ROOT/graph-ui/dist" ]] && UI_DIRS+=("$ROOT/graph-ui/dist")
[[ -d "$ROOT/graph-ui/src" ]] && UI_DIRS+=("$ROOT/graph-ui/src")

if [[ ${#UI_DIRS[@]} -eq 0 ]]; then
    echo "SKIP: No graph-ui directory found."
else
    for UI_DIR in "${UI_DIRS[@]}"; do
        echo "Scanning: $UI_DIR"

        # A1: No external domains in JS/CSS/TS source code.
        # For src/ (our code): strict — any external URL is blocked.
        # For dist/ (bundled npm output): skip inline URL scan — minified JS
        # contains hundreds of string-constant URLs from libraries (React error
        # pages, W3C namespace URIs, CDN references, OSS credits) that are never
        # fetched at runtime. Scanning them is noise. Structural checks (A2-A6)
        # still apply to dist/.
        is_dist=false
        [[ "$UI_DIR" == *"/dist" || "$UI_DIR" == *"/dist/" ]] && is_dist=true

        if ! $is_dist; then
            echo "  Checking for external domains (source)..."
            if find "$UI_DIR" -type f \( -name '*.js' -o -name '*.ts' -o -name '*.tsx' -o -name '*.css' \) -exec grep -lE 'https?://' {} \; 2>/dev/null | head -20 > "$SEC_TMPDIR/urls"; then
                while IFS= read -r file; do
                    relfile="${file#"$ROOT/"}"
                    grep -onE 'https?://[^\s"'"'"')]+' "$file" 2>/dev/null | while IFS=: read -r lineno url; do
                        case "$url" in
                            http://localhost*|http://127.0.0.1*|https://localhost*|https://127.0.0.1*)
                                ;; # OK — local dev/runtime
                            *)
                                echo "  BLOCKED: ${relfile}:${lineno}: External URL: $url"
                                touch "$SEC_TMPDIR/fail_flag"
                                ;;
                        esac
                    done
                done < "$SEC_TMPDIR/urls"
            fi
            [[ -f "$SEC_TMPDIR/fail_flag" ]] && FAIL=1 && rm -f "$SEC_TMPDIR/fail_flag"
        else
            echo "  Skipping inline URL scan for dist/ (bundled library strings)."
            echo "  Structural checks (script loads, tracking, eval, iframes) still apply."
        fi

        # A2: No external script/link loads in HTML
        echo "  Checking for external script/link loads..."
        if find "$UI_DIR" -type f -name '*.html' -exec grep -lE '<script\s+src=|<link\s+href=' {} \; 2>/dev/null > "$SEC_TMPDIR/scripts"; then
            while IFS= read -r file; do
                relfile="${file#"$ROOT/"}"
                if grep -nE '<script\s+src="https?://|<link\s+href="https?://' "$file" 2>/dev/null \
                    | grep -v 'localhost' | grep -v '127.0.0.1' \
                    | grep -v 'fonts.googleapis.com' | grep -v 'fonts.gstatic.com'; then
                    echo "  BLOCKED: ${relfile}: External script/link load detected"
                    FAIL=1
                fi
            done < "$SEC_TMPDIR/scripts"
        fi

        # A3: No tracking/analytics
        echo "  Checking for tracking/analytics..."
        TRACKING='google-analytics|gtag|mixpanel|segment\.com|hotjar|sentry\.io|plausible|posthog'
        if find "$UI_DIR" -type f \( -name '*.js' -o -name '*.ts' -o -name '*.tsx' -o -name '*.html' \) \
            -exec grep -lE "$TRACKING" {} \; 2>/dev/null > "$SEC_TMPDIR/track"; then
            while IFS= read -r file; do
                relfile="${file#"$ROOT/"}"
                echo "  BLOCKED: ${relfile}: Tracking/analytics reference found"
                grep -nE "$TRACKING" "$file" | head -3
                FAIL=1
            done < "$SEC_TMPDIR/track"
        fi

        # A4: No hidden iframes
        echo "  Checking for iframes..."
        if find "$UI_DIR" -type f -name '*.html' -exec grep -li '<iframe' {} \; 2>/dev/null > "$SEC_TMPDIR/iframe"; then
            while IFS= read -r file; do
                relfile="${file#"$ROOT/"}"
                echo "  BLOCKED: ${relfile}: iframe detected"
                FAIL=1
            done < "$SEC_TMPDIR/iframe"
        fi

        # A5: No eval/Function constructor in JS
        echo "  Checking for eval/Function constructor..."
        if find "$UI_DIR" -type f \( -name '*.js' -o -name '*.ts' -o -name '*.tsx' \) \
            -exec grep -nE '\beval\s*\(|new\s+Function\s*\(' {} \; 2>/dev/null | grep -v node_modules | grep -v '\.test\.' > "$SEC_TMPDIR/eval"; then
            while IFS= read -r match; do
                echo "  REVIEW: eval/Function found: $match"
            done < "$SEC_TMPDIR/eval"
        fi

        # A6: No WebSocket to external
        echo "  Checking for external WebSocket connections..."
        if find "$UI_DIR" -type f \( -name '*.js' -o -name '*.ts' -o -name '*.tsx' \) \
            -exec grep -nE 'wss?://' {} \; 2>/dev/null | grep -v 'localhost' | grep -v '127.0.0.1' > "$SEC_TMPDIR/ws"; then
            while IFS= read -r match; do
                echo "  BLOCKED: External WebSocket: $match"
                FAIL=1
            done < "$SEC_TMPDIR/ws"
        fi
    done
fi

# ── B. HTTP server binding check ─────────────────────────────────

echo ""
echo "--- B. HTTP server binding check ---"

HTTP_SERVER="$ROOT/src/ui/http_server.c"
if [[ -f "$HTTP_SERVER" ]]; then
    # Must bind to 127.0.0.1 only
    if grep -q '127\.0\.0\.1' "$HTTP_SERVER"; then
        echo "OK: Server binds to 127.0.0.1"
    else
        echo "BLOCKED: No 127.0.0.1 binding found in http_server.c"
        FAIL=1
    fi

    # Must NOT bind to 0.0.0.0 or INADDR_ANY
    if grep -E '0\.0\.0\.0|INADDR_ANY|in6addr_any' "$HTTP_SERVER" | grep -v '^\s*//' | grep -v '^\s*\*' > /dev/null 2>&1; then
        echo "BLOCKED: Server may bind to all interfaces (0.0.0.0/INADDR_ANY found)"
        FAIL=1
    else
        echo "OK: No 0.0.0.0/INADDR_ANY binding"
    fi
else
    echo "SKIP: http_server.c not found"
fi

# ── C. RPC proxy scope check ─────────────────────────────────────

echo ""
echo "--- C. RPC proxy scope check ---"

if [[ -f "$HTTP_SERVER" ]]; then
    # The HTTP handler should not directly call system()/popen()
    # (fork/execl for indexing is allowed as it's tracked)
    if grep -n 'system(' "$HTTP_SERVER" | grep -v '^\s*//' | grep -v '^\s*\*' > /dev/null 2>&1; then
        echo "BLOCKED: system() call found in HTTP server (use subprocess instead)"
        FAIL=1
    else
        echo "OK: No system() calls in HTTP handler"
    fi
else
    echo "SKIP: http_server.c not found"
fi

# ── D. CORS check ────────────────────────────────────────────────

echo ""
echo "--- D. CORS check ---"

if [[ -f "$HTTP_SERVER" ]]; then
    if grep -E 'Allow-Origin:\s*\*' "$HTTP_SERVER" | grep -v '^\s*//' | grep -v '^\s*\*' > /dev/null 2>&1; then
        echo "BLOCKED: CORS wildcard (Access-Control-Allow-Origin: *) found"
        echo "  This allows any website to access the local server."
        FAIL=1
    else
        echo "OK: No CORS wildcard found"
    fi

    # Check that CORS reflects localhost origins
    if grep -q 'localhost' "$HTTP_SERVER" && grep -q 'update_cors\|Access-Control-Allow-Origin' "$HTTP_SERVER"; then
        echo "OK: CORS appears to validate localhost origins"
    fi
else
    echo "SKIP: http_server.c not found"
fi

# ── Summary ──────────────────────────────────────────────────────

echo ""
if [[ $FAIL -ne 0 ]]; then
    echo "=== UI SECURITY AUDIT FAILED ==="
    exit 1
fi

echo "=== UI security audit passed ==="
