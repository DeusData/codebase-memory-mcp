#!/usr/bin/env python3
"""autoclaude memory-guard — a PreToolUse hook for Claude Code (Write|Edit).

Guardrails belong inside the execution loop, not bolted on after. This is the
deterministic pre-write check for the memory layer: if a Write/Edit targets a
`memory/` path, scan the payload for secrets (keys, tokens, private keys) and
DENY the write. Secrets must never land in a durable memory store — it is the
top risk for a long-lived agent (the store outlives the secret's validity and
gets replayed into every later prompt).

Only fires on memory paths, only on secret patterns — so it stays out of the way
for normal code edits. Any error → exit 0 (never wedge the agent).
"""
from __future__ import annotations

import json
import re
import sys

# High-signal secret patterns. Deliberately narrow to avoid false positives on
# ordinary prose ("api_key" alone is fine; an actual key value is not).
SECRET_PATTERNS = [
    (re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----"), "private key block"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "AWS access key id"),
    (re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"), "OpenAI-style secret key"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"), "GitHub token"),
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), "Slack token"),
    (re.compile(r"\bya29\.[A-Za-z0-9_\-]{20,}\b"), "Google OAuth token"),
    (re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b"), "JWT"),
    (re.compile(r"(?i)\b(?:secret|password|passwd|api[_-]?key|token)\b\s*[:=]\s*['\"]?[A-Za-z0-9_\-/+]{16,}"), "inline credential assignment"),
]


def scan(text: str):
    for pat, label in SECRET_PATTERNS:
        if pat.search(text):
            return label
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool = payload.get("tool_name", "")
    if tool not in ("Write", "Edit", "MultiEdit", "NotebookEdit"):
        return 0

    inp = payload.get("tool_input", {}) or {}
    path = str(inp.get("file_path") or inp.get("notebook_path") or "")
    # Only guard the memory layer.
    if "/memory/" not in path.replace("\\", "/") and not path.rstrip("/").endswith("/memory"):
        return 0

    # Gather all writable text from the payload shape.
    chunks = []
    for k in ("content", "new_string", "new_source"):
        v = inp.get(k)
        if isinstance(v, str):
            chunks.append(v)
    for edit in inp.get("edits", []) or []:
        if isinstance(edit, dict) and isinstance(edit.get("new_string"), str):
            chunks.append(edit["new_string"])
    text = "\n".join(chunks)
    if not text:
        return 0

    label = scan(text)
    if label:
        out = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"[autoclaude memory-guard] Заблокирована запись в память: похоже на {label}. "
                    "Секреты нельзя хранить в долговременной памяти — они переживают свою валидность "
                    "и попадают в каждый следующий prompt. Сохрани факт без секрета "
                    "(ссылку на хранилище ключей, а не сам ключ)."
                ),
            }
        }
        print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
