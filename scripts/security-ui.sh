#!/usr/bin/env bash
set -euo pipefail

# Layer 6: Graph UI security audit.
#
# Audits:
#   A. Frontend asset scan (embedded JS/CSS/HTML or graph-ui/dist/)
#   B. HTTP server binding (localhost by default; non-loopback is explicit)
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

# The UI server is split across two first-party files:
#   httpd.c        — socket transport (binding, parsing)
#   http_server.c  — routing + endpoint handlers (CORS policy)
# Both MUST exist — a missing file means the audit target moved and the
# audit would go blind, so that is a hard failure, not a skip.
HTTPD="$ROOT/src/ui/httpd.c"
HTTP_SERVER="$ROOT/src/ui/http_server.c"
UI_CONFIG="$ROOT/src/ui/config.h"
UI_CONFIG_C="$ROOT/src/ui/config.c"
MAIN="$ROOT/src/main.c"
for f in "$HTTPD" "$HTTP_SERVER"; do
    if [[ ! -f "$f" ]]; then
        echo "BLOCKED: expected UI server source missing: $f"
        FAIL=1
    fi
done

if [[ -f "$HTTPD" ]]; then
    # The safe default must remain localhost, including when loading an old
    # config file that has no host field.
    if [[ -f "$UI_CONFIG" ]] && grep -qE '#define CBM_UI_DEFAULT_HOST "127\.0\.0\.1"' "$UI_CONFIG" && \
       [[ -f "$UI_CONFIG_C" ]] && grep -q 'CBM_UI_DEFAULT_HOST' "$UI_CONFIG_C"; then
        echo "OK: Default UI host is 127.0.0.1"
    else
        echo "BLOCKED: Default UI host is not provably 127.0.0.1"
        FAIL=1
    fi

    # Non-loopback binding must travel through the explicit numeric IPv4 flag
    # and the validated host parameter; this prevents implicit exposure.
    if grep -q '"--host="' "$MAIN" && grep -q 'cbm_ui_host_is_valid' "$MAIN" && \
       grep -q 'cbm_httpd_listen(const char \*host, int port)' "$HTTPD" && \
       grep -q 'inet_pton(AF_INET' "$HTTPD"; then
        echo "OK: Non-loopback binding requires explicit validated IPv4 configuration"
    else
        echo "BLOCKED: Host configuration does not prove explicit validated IPv4 binding"
        FAIL=1
    fi

    # A non-loopback bind must be visibly warned about at the point of use.
    if grep -q 'cbm_ui_host_is_loopback' "$HTTP_SERVER" && \
       grep -qi 'no authentication' "$HTTP_SERVER" && \
       grep -qi 'reachable over the network' "$HTTP_SERVER" && \
       grep -qi 'codebase graph data' "$HTTP_SERVER" && \
       grep -qi 'firewall' "$HTTP_SERVER"; then
        echo "OK: Non-loopback binding emits a prominent security warning"
    else
        echo "BLOCKED: Non-loopback security warning is missing or incomplete"
        FAIL=1
    fi
fi

# ── C. RPC proxy scope check ─────────────────────────────────────

echo ""
echo "--- C. RPC proxy scope check ---"

if [[ -f "$HTTP_SERVER" ]]; then
    # The HTTP handler should not directly call system()/popen()
    # (fork/execl for indexing is allowed as it's tracked)
    if cat "$HTTPD" "$HTTP_SERVER" 2>/dev/null | grep -n 'system(' | grep -v '^\s*//' | grep -v '^\s*\*' > /dev/null 2>&1; then
        echo "BLOCKED: system() call found in HTTP server (use subprocess instead)"
        FAIL=1
    else
        echo "OK: No system() calls in HTTP handler"
    fi
fi

# ── D. CORS check ────────────────────────────────────────────────

echo ""
echo "--- D. CORS check ---"

if [[ -f "$HTTP_SERVER" ]]; then
    if cat "$HTTPD" "$HTTP_SERVER" 2>/dev/null | grep -E 'Allow-Origin:\s*\*' | grep -v '^\s*//' | grep -v '^\s*\*' > /dev/null 2>&1; then
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
fi

# ── Summary ──────────────────────────────────────────────────────

echo ""
if [[ $FAIL -ne 0 ]]; then
    echo "=== UI SECURITY AUDIT FAILED ==="
    exit 1
fi

echo "=== UI security audit passed ==="
