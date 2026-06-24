#!/bin/bash
# Verify NOLINT(misc-no-recursion) only appears on whitelisted functions.
WHITELIST="src/foundation/recursion_whitelist.h"
if [ ! -f "$WHITELIST" ]; then
    echo "ERROR: $WHITELIST not found"; exit 1
fi
ALLOWED=$(grep -oE '"[a-zA-Z_][a-zA-Z0-9_]*"' "$WHITELIST" | tr -d '"' | sort -u)

HITS=$(grep -rn 'NOLINT(misc-no-recursion)' src/ internal/cbm/*.c internal/cbm/*.h 2>/dev/null \
    | grep -v vendored | grep -v recursion_whitelist)
if [ -z "$HITS" ]; then exit 0; fi

FAIL=0
while IFS= read -r line; do
    FILE=$(echo "$line" | cut -d: -f1)
    LN=$(echo "$line" | cut -d: -f2)
    # Check current line AND 5 lines above for the function name
    CONTEXT=$(sed -n "$((LN > 5 ? LN - 5 : 1)),${LN}p" "$FILE")
    FOUND=0
    for fn in $ALLOWED; do
        if echo "$CONTEXT" | grep -qw "$fn"; then
            FOUND=1; break
        fi
    done
    if [ "$FOUND" -eq 0 ]; then
        echo "ERROR: NOLINT(misc-no-recursion) on non-whitelisted function:"
        echo "  $line"
        FAIL=1
    fi
done <<< "$HITS"
exit $FAIL
