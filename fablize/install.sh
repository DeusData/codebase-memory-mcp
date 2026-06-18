#!/usr/bin/env bash
# fablize install — apply the procedure layer to a project (any agent).
# Companion to the codebase-memory-mcp (memory layer) install. Additive and idempotent:
# copies packs+scripts in, appends the operating block to whatever instruction file the
# agent reads, and registers the destructive-action guard for Claude Code if present.
# Usage: bash fablize/install.sh [target-project-dir]   (default: current directory)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:-$PWD}"
echo "fablize (procedure layer) → $TARGET"

mkdir -p "$TARGET/.fablize-disciplines/packs" "$TARGET/.fablize-disciplines/scripts"
cp "$HERE/packs/"*.txt   "$TARGET/.fablize-disciplines/packs/"
cp "$HERE/scripts/"*.py  "$TARGET/.fablize-disciplines/scripts/"
cp "$HERE/hooks/destructive_guard.py" "$TARGET/.fablize-disciplines/" 2>/dev/null || true
echo "  ✓ packs + scripts → .fablize-disciplines/"

# Append the operating block to any instruction file the agent already uses, else AGENTS.md.
block="$HERE/AGENTS.md"
wrote=0
for f in AGENTS.md CLAUDE.md .cursorrules .github/copilot-instructions.md GEMINI.md; do
  path="$TARGET/$f"
  if [ -f "$path" ]; then
    if ! grep -q "fablize — operating disciplines" "$path" 2>/dev/null; then
      mkdir -p "$(dirname "$path")"
      { printf '\n\n'; cat "$block"; } >> "$path"
      echo "  ✓ appended disciplines to $f"
    else
      echo "  = $f already has fablize disciplines"
    fi
    wrote=1
  fi
done
if [ "$wrote" -eq 0 ]; then
  cp "$block" "$TARGET/AGENTS.md"
  echo "  ✓ created AGENTS.md"
fi

# Register the destructive-action guard for Claude Code, if its settings file is present.
SETTINGS="$HOME/.claude/settings.json"
if command -v python3 >/dev/null 2>&1 && [ -f "$SETTINGS" ]; then
  python3 - "$SETTINGS" "$TARGET/.fablize-disciplines/destructive_guard.py" <<'PY' || true
import json, os, sys
settings, guard = sys.argv[1], sys.argv[2]
try:
    data = json.load(open(settings, encoding="utf-8"))
except (OSError, ValueError):
    raise SystemExit(0)
cmd = f'python3 "{guard}"'
hooks = data.setdefault("hooks", {}).setdefault("PreToolUse", [])
blob = json.dumps(hooks)
if "destructive_guard.py" in blob:
    print("  = destructive guard already registered (Claude Code)"); raise SystemExit(0)
hooks.append({"matcher": "Bash", "hooks": [{"type": "command", "command": cmd, "timeout": 10}]})
json.dump(data, open(settings, "w", encoding="utf-8"), indent=2)
print("  ✓ destructive guard registered (Claude Code PreToolUse)")
PY
fi

echo "Done. The agent now has the fablize disciplines wired to the memory tools (see INTEGRATION.md)."
