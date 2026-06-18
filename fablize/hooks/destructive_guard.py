#!/usr/bin/env python3
"""fablize destructive-action guard — a deterministic PreToolUse hook.

The "confirm before destructive or hard-to-reverse actions" rule lives in the operating
block as text, which a model can skip. This hook makes it a *preventive control*: it
inspects Bash commands before they run and forces a human approval prompt for the
genuinely dangerous, hard-to-reverse ones (recursive force-delete, force-push, history
rewrite, disk wipe, destructive SQL, etc.).

Protocol: reads the PreToolUse payload on stdin, emits a permission decision on stdout.
  - "ask"  → Claude Code prompts the user to approve before running (default for matches).
  - silent → exit 0 with no output lets the command proceed normally.
It never hard-blocks (deny) — the user stays in control; it only inserts a checkpoint.
"""
import json
import re
import sys

# (compiled pattern, human reason). Order doesn't matter; first match wins for the message.
RULES = [
    (r"\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+)*-?[a-zA-Z]*f|\brm\s+-[a-zA-Z]*f[a-zA-Z]*r", "recursive/forced file deletion (rm -rf)"),
    (r"\brm\s+-[a-zA-Z]*r[a-zA-Z]*\s+(/|~|\$HOME|\.)\s*$", "recursive delete of a top-level path"),
    (r"\bgit\s+push\b.*(--force\b|-f\b)", "git force-push (rewrites remote history)"),
    (r"\bgit\s+(reset\s+--hard|clean\s+-[a-zA-Z]*f|filter-branch|filter-repo)\b", "git history/working-tree destruction"),
    (r"\bgit\s+branch\s+-D\b", "force-delete of a git branch"),
    (r"\b(drop|truncate)\s+(table|database|schema)\b", "destructive SQL (DROP/TRUNCATE)"),
    (r"\b(mkfs|dd\s+if=|shred|wipefs)\b", "disk/partition wipe"),
    (r"\b(kubectl|helm)\s+delete\b", "Kubernetes resource deletion"),
    (r"\b(terraform|tofu)\s+destroy\b", "infrastructure teardown (terraform destroy)"),
    (r":\(\)\s*\{\s*:\|:&\s*\}", "fork bomb"),
    (r"\bchmod\s+-R\b|\bchown\s+-R\b", "recursive permission/ownership change"),
    (r">\s*/dev/sd[a-z]", "raw write to a block device"),
]
COMPILED = [(re.compile(p, re.I), why) for p, why in RULES]


def match(command):
    for rx, why in COMPILED:
        if rx.search(command):
            return why
    return None


def main():
    try:
        payload = json.load(sys.stdin)
    except (ValueError, OSError):
        sys.exit(0)  # malformed input — do not interfere
    if payload.get("tool_name") != "Bash":
        sys.exit(0)
    command = (payload.get("tool_input") or {}).get("command", "")
    if not command:
        sys.exit(0)
    why = match(command)
    if not why:
        sys.exit(0)
    out = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": f"fablize guard: {why}. Confirm this hard-to-reverse action before running.",
        }
    }
    print(json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()
