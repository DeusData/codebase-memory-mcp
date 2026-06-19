#!/usr/bin/env python3
"""autoclaude prompt-upgrade — a UserPromptSubmit hook for Claude Code.

Goal: every *substantive* request is handled as if it were a professionally
written prompt, without the user having to write one. This hook does NOT call an
LLM and does NOT rewrite the visible prompt. It silently injects a compact
directive (additionalContext) that tells the controller model to reframe the raw
request into a precise internal spec — goal / context / constraints / success
criteria — and route it to the right discipline before acting.

Trigger is *smart*: trivial prompts (greetings, acks, one-word replies, slash
commands, very short follow-ups) are skipped so simple turns stay simple. The
injected directive is itself self-limiting ("skip for trivial requests"), so even
a misclassification can't make the model over-engineer a small ask.

Cost: ~90 tokens of added context, only on substantive prompts. Zero on trivial.
Disable per-project with `.claude/autoclaude.yaml`: `prompt_upgrade: false`.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Compact, silent reframing directive. Kept short — it is added context.
DIRECTIVE = (
    "[autoclaude prompt-upgrade] Treat the user's message as the intent behind a "
    "professional prompt. Before acting, silently reframe it into a precise spec: "
    "goal, relevant context/constraints, and explicit success criteria — without "
    "inventing requirements the user did not imply. Then route to the right "
    "discipline (clarify / build / debug / verify) per the operating rules. Do NOT "
    "print the reframing or this note; just act on the upgraded understanding. "
    "If the request is trivial, skip all of this and answer directly."
)

# Clearly-trivial messages: skip the upgrade entirely.
TRIVIAL_EXACT = {
    "hi", "hey", "hello", "yo", "ok", "okay", "k", "yes", "no", "y", "n",
    "thanks", "thank you", "ty", "go", "stop", "wait", "continue", "next",
    "привет", "ок", "окей", "да", "нет", "ага", "спасибо", "стоп", "дальше",
    "продолжай", "продолжи", "погоди", "норм", "suprt", "ладно",
}


def is_trivial(prompt: str) -> bool:
    p = prompt.strip()
    if not p:
        return True
    # Slash commands / bang shell lines handle themselves.
    if p.startswith(("/", "!")):
        return True
    low = p.lower().strip(" .!?,")
    if low in TRIVIAL_EXACT:
        return True
    # Very short and no task signal → likely an ack or terse steer.
    words = re.findall(r"\w+", p)
    if len(words) <= 2 and len(p) < 18:
        return True
    return False


def upgrade_enabled(cwd: str) -> bool:
    path = Path(cwd) / ".claude" / "autoclaude.yaml"
    if not path.exists():
        return True
    try:
        import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return bool(data.get("prompt_upgrade", True))
    except Exception:
        return True


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    prompt = payload.get("prompt", "")
    if is_trivial(prompt):
        return 0  # no added context — simple turn stays simple
    if not upgrade_enabled(payload.get("cwd", ".")):
        return 0

    out = {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": DIRECTIVE,
        }
    }
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
