#!/usr/bin/env bash
# autoclaude installer — wires token-economy into Claude Code globally (all
# sessions/flows). Idempotent: safe to re-run; uninstall with --uninstall.
#
#   ./install.sh                 # install globally + .claudeignore in $PWD
#   ./install.sh /path/to/proj   # also drop .claudeignore there
#   ./install.sh --uninstall      # remove the managed CLAUDE.md block + hook
set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLAUDE_DIR="$HOME/.claude"
CLAUDEMD="$CLAUDE_DIR/CLAUDE.md"
HOOK_DST="$CLAUDE_DIR/hooks/quota_guard.py"
BEGIN="<!-- autoclaude:economy:begin -->"
END="<!-- autoclaude:economy:end -->"
MEM_BEGIN="<!-- autoclaude:memory:begin -->"
MEM_END="<!-- autoclaude:memory:end -->"
EPISODIC_DST="$CLAUDE_DIR/hooks/episodic_logger.py"
MEMGUARD_DST="$CLAUDE_DIR/hooks/memory_guard.py"
PROMPTUP_DST="$CLAUDE_DIR/hooks/prompt_upgrade.py"

strip_block() {  # remove a managed block ($1=begin $2=end) from CLAUDE.md
  [ -f "$CLAUDEMD" ] || return 0
  awk -v b="$1" -v e="$2" '
    $0==b{skip=1} skip&&$0==e{skip=0;next} !skip' "$CLAUDEMD" > "$CLAUDEMD.tmp"
  mv "$CLAUDEMD.tmp" "$CLAUDEMD"
}

if [ "${1:-}" = "--uninstall" ]; then
  strip_block "$BEGIN" "$END"
  strip_block "$MEM_BEGIN" "$MEM_END"
  rm -f "$HOOK_DST" "$EPISODIC_DST" "$MEMGUARD_DST" "$PROMPTUP_DST"
  rm -rf "$CLAUDE_DIR/skills/memory-prune"
  echo "autoclaude removed (economy + memory blocks, hooks, /memory-prune). Drop the hook entries from settings.json manually if set."
  exit 0
fi

mkdir -p "$CLAUDE_DIR/hooks"

# 1) economy + memory rules -> managed blocks in global CLAUDE.md (replace if present)
strip_block "$BEGIN" "$END"
{ echo "$BEGIN"; cat "$SRC/economy.md"; echo "$END"; } >> "$CLAUDEMD"
echo "✓ economy rules injected into $CLAUDEMD"
strip_block "$MEM_BEGIN" "$MEM_END"
{ echo "$MEM_BEGIN"; cat "$SRC/memory.md"; echo "$MEM_END"; } >> "$CLAUDEMD"
echo "✓ memory-discipline rules injected into $CLAUDEMD"

# 2) quota-guard hook -> global hooks dir
cp "$SRC/.claude/hooks/quota_guard.py" "$HOOK_DST"
chmod +x "$HOOK_DST"
echo "✓ quota_guard hook installed at $HOOK_DST"

# 2b) wire the PreToolUse hook into ~/.claude/settings.json (idempotent)
SETTINGS="$CLAUDE_DIR/settings.json" python3 - <<'PY'
import json, os
path = os.environ["SETTINGS"]
data = {}
if os.path.exists(path):
    try:
        with open(path) as fh:
            data = json.load(fh) or {}
    except Exception:
        print(f"! {path} is not valid JSON — left untouched; add PreToolUse manually"); raise SystemExit(0)
cmd = 'python3 "$HOME/.claude/hooks/quota_guard.py"'
hooks = data.setdefault("hooks", {})
pre = hooks.setdefault("PreToolUse", [])
already = any(
    "quota_guard.py" in h.get("command", "")
    for entry in pre for h in entry.get("hooks", [])
)
if already:
    print("• settings.json PreToolUse already wired — skipped")
else:
    pre.append({"matcher": "*", "hooks": [
        {"type": "command", "command": cmd, "timeout": 10,
         "statusMessage": "Checking quota window..."}]})
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False); fh.write("\n")
    print("✓ settings.json PreToolUse wired to quota_guard")
PY

# 2c) memory-layer hooks -> global hooks dir
cp "$SRC/.claude/hooks/episodic_logger.py" "$EPISODIC_DST"; chmod +x "$EPISODIC_DST"
cp "$SRC/.claude/hooks/memory_guard.py" "$MEMGUARD_DST"; chmod +x "$MEMGUARD_DST"
cp "$SRC/.claude/hooks/prompt_upgrade.py" "$PROMPTUP_DST"; chmod +x "$PROMPTUP_DST"
echo "✓ episodic-logger + memory-guard + prompt-upgrade hooks installed"

# 2d) wire memory hooks into ~/.claude/settings.json (idempotent)
SETTINGS="$CLAUDE_DIR/settings.json" python3 - <<'PY'
import json, os
path = os.environ["SETTINGS"]
data = {}
if os.path.exists(path):
    try:
        with open(path) as fh:
            data = json.load(fh) or {}
    except Exception:
        print(f"! {path} is not valid JSON — left untouched; add memory hooks manually"); raise SystemExit(0)
hooks = data.setdefault("hooks", {})

def ensure(event, marker, entry):
    arr = hooks.setdefault(event, [])
    if any(marker in h.get("command", "") for e in arr for h in e.get("hooks", [])):
        print(f"• settings.json {event} already wired ({marker}) — skipped"); return
    arr.append(entry); print(f"✓ settings.json {event} wired ({marker})")

ensure("Stop", "episodic_logger.py", {
    "matcher": "", "hooks": [{"type": "command",
        "command": 'python3 "$HOME/.claude/hooks/episodic_logger.py"',
        "timeout": 10, "statusMessage": "Recording episode..."}]})
ensure("PreToolUse", "memory_guard.py", {
    "matcher": "Write|Edit|MultiEdit|NotebookEdit", "hooks": [{"type": "command",
        "command": 'python3 "$HOME/.claude/hooks/memory_guard.py"',
        "timeout": 10, "statusMessage": "Memory guard..."}]})
ensure("UserPromptSubmit", "prompt_upgrade.py", {
    "matcher": "", "hooks": [{"type": "command",
        "command": 'python3 "$HOME/.claude/hooks/prompt_upgrade.py"',
        "timeout": 10}]})

with open(path, "w") as fh:
    json.dump(data, fh, indent=2, ensure_ascii=False); fh.write("\n")
PY

# 2e) memory-prune skill -> global skills dir
SKILL_DST="$CLAUDE_DIR/skills/memory-prune"
mkdir -p "$SKILL_DST"
cp "$SRC/skills-local/memory-prune/SKILL.md" "$SKILL_DST/SKILL.md"
cp "$SRC/skills-local/memory-prune/scan.py" "$SKILL_DST/scan.py"
chmod +x "$SKILL_DST/scan.py"
echo "✓ /memory-prune skill installed at $SKILL_DST"

# 3) .claudeignore into target project (arg or cwd)
TARGET="${1:-$PWD}"
if [ -d "$TARGET" ]; then
  if [ -f "$TARGET/.claudeignore" ]; then
    echo "• $TARGET/.claudeignore exists — left untouched"
  else
    cp "$SRC/.claudeignore.template" "$TARGET/.claudeignore"
    echo "✓ .claudeignore added to $TARGET"
  fi
fi
echo "Done. Applies to all new Claude Code sessions."
