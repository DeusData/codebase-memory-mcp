#!/usr/bin/env bash
set -euo pipefail

# Install the repo's git hooks into .git/hooks (local, per-clone).
# Currently: commit-msg (strict DCO sign-off enforcement).

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_DIR="$(git -C "$ROOT" rev-parse --git-path hooks)"

install -m 755 "$ROOT/scripts/git-hooks/commit-msg" "$HOOKS_DIR/commit-msg"
echo "installed: $HOOKS_DIR/commit-msg (DCO sign-off required on every commit)"
