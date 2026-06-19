#!/usr/bin/env python3
"""autoclaude quota-guard — a PreToolUse hook for Claude Code on a *subscription*.

On a subscription the scarce resource is the rate-limit / quota window, NOT
dollars. So this guard estimates the tokens burned in the current session (from
the transcript Claude Code hands every hook) and:

  - warns (stderr) once a soft fraction of the window cap is spent, nudging the
    orchestrator to stop fanning out parallel subagents;
  - denies new tool calls once the hard cap is hit, so a runaway fan-out can't
    drain the whole quota window in one prompt.

This is the subscription re-think of the hermes-economizer ledger: same shape,
metric swapped from USD to tokens/quota.

Config (optional): .claude/autoclaude.yaml next to the project, keys:
    cap_tokens: 250000     # hard cap for the window (deny above this)
    warn_ratio: 0.8        # warn once this fraction is spent
Defaults are used if the file or PyYAML is missing.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

DEFAULTS = {"cap_tokens": 250_000, "warn_ratio": 0.8}


def load_config(cwd: str) -> dict:
    cfg = dict(DEFAULTS)
    path = Path(cwd) / ".claude" / "autoclaude.yaml"
    if path.exists():
        try:
            import yaml  # optional
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            cfg.update({k: data[k] for k in ("cap_tokens", "warn_ratio") if k in data})
        except Exception:
            pass
    return cfg


def estimate_session_tokens(transcript_path: str) -> int:
    """~4 chars/token over all message text in the session transcript (JSONL)."""
    p = Path(transcript_path)
    if not p.exists():
        return 0
    chars = 0
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg = entry.get("message", entry)
        content = msg.get("content", "")
        if isinstance(content, str):
            chars += len(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    chars += len(str(block.get("text", "")))
                    chars += len(json.dumps(block.get("input", ""))) if "input" in block else 0
    return chars // 4


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0  # never block the agent on a malformed payload

    cwd = payload.get("cwd", ".")
    transcript = payload.get("transcript_path", "")
    cfg = load_config(cwd)

    used = estimate_session_tokens(transcript)
    cap = int(cfg["cap_tokens"])
    warn_at = int(cap * float(cfg["warn_ratio"]))

    if used >= cap:
        # Window spent: ask the user before more tool work (soft guardrail).
        out = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "ask",
                "permissionDecisionReason": (
                    f"[autoclaude quota] Окно квоты исчерпано: ~{used:,} / {cap:,} токенов. "
                    "Стоит свернуться: подвести итог сделанного и продолжить позже. "
                    "Разрешить ещё один tool-call?"
                ),
            }
        }
        print(json.dumps(out, ensure_ascii=False))
        return 0

    if used >= warn_at:
        # Soft warning — surfaced to the user/agent via stderr; does not block.
        pct = used / cap * 100
        print(
            f"[autoclaude quota] ~{used:,} / {cap:,} токенов ({pct:.0f}%) — "
            "близко к лимиту окна, сворачивай параллельные субагенты.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
