#!/usr/bin/env bash
set -euo pipefail

# DCO enforcement: every commit in the given range must carry a
# Signed-off-by trailer whose email matches the commit author
# (Developer Certificate of Origin 1.1 — see the DCO file).
#
# Usage: check-dco.sh <range>          e.g. origin/main..HEAD, sha1..sha2
#
# Exemptions (same as the standard DCO checks): merge commits and
# bot-authored commits (author name ending in [bot]).

RANGE="${1:?usage: check-dco.sh <commit-range>}"

FAIL=0
CHECKED=0
while IFS= read -r sha; do
    # Skip merge commits
    nparents=$(git rev-list --no-walk --parents -n1 "$sha" | wc -w)
    if [ "$nparents" -gt 2 ]; then
        continue
    fi
    author_name=$(git log -1 --format='%an' "$sha")
    case "$author_name" in
        *"[bot]") continue ;;
    esac
    CHECKED=$((CHECKED + 1))
    author_email=$(git log -1 --format='%ae' "$sha")
    trailers=$(git log -1 --format='%(trailers:key=Signed-off-by,valueonly)' "$sha")
    if ! printf '%s' "$trailers" | grep -qiF "<$author_email>"; then
        echo "BLOCKED: $sha lacks a Signed-off-by matching its author:"
        git log -1 --format='  author: %an <%ae>%n  subject: %s' "$sha"
        echo "  fix: git commit --amend -s   (or: git rebase --signoff <base>)"
        FAIL=1
    fi
done < <(git rev-list "$RANGE")

if [ "$FAIL" -ne 0 ]; then
    echo "=== DCO CHECK FAILED — every commit must be signed off (git commit -s) ==="
    echo "=== See the DCO file and CONTRIBUTING.md ==="
    exit 1
fi
echo "OK: $CHECKED commit(s) in $RANGE carry a valid Signed-off-by"
