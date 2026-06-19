#!/usr/bin/env python3
"""autoclaude episodic-logger — a Stop hook for Claude Code.

The missing memory layer: episodic records. On each Stop, distil the session
transcript into one raw episode (goal / tools used / outcome) and append it to
the project's `memory/episodes/YYYY-MM.jsonl`. These are *raw* records — the doc
keeps raw event logs separate from reflective summaries, so this hook never
editorialises; it just captures what happened, deduped by session id.

Never blocks the agent: any error → exit 0. Trivial sessions (no tools, no real
task) are skipped to keep the log signal-dense.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

MAX_GOAL = 280   # chars kept from the task statement
MAX_RESULT = 280


def parse_transcript(path: Path):
    """Return (first_user_text, last_assistant_text, Counter(tool_name))."""
    first_user = ""
    last_assistant = ""
    tools: Counter = Counter()
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg = entry.get("message", entry)
        role = msg.get("role") or entry.get("type")
        content = msg.get("content", "")
        text_parts = []
        if isinstance(content, str):
            text_parts.append(content)
        elif isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "tool_use" and block.get("name"):
                    tools[block["name"]] += 1
                if block.get("text"):
                    text_parts.append(str(block["text"]))
        text = " ".join(t.strip() for t in text_parts if t and t.strip())
        if not text:
            continue
        if role in ("user", "human") and not first_user:
            # skip tool-result / system-reminder noise carried as "user"
            if not text.lstrip().startswith(("<", "[")):
                first_user = text
        if role == "assistant":
            last_assistant = text
    return first_user, last_assistant, tools


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    transcript = payload.get("transcript_path", "")
    if not transcript:
        return 0
    tpath = Path(transcript)
    if not tpath.exists():
        return 0

    try:
        goal, result, tools = parse_transcript(tpath)
    except Exception:
        return 0

    # Skip trivial sessions: nothing was done worth remembering.
    if not goal or not tools:
        return 0

    mem_dir = tpath.parent / "memory" / "episodes"
    try:
        mem_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        return 0

    now = datetime.now(timezone.utc)
    record = {
        "session_id": payload.get("session_id", tpath.stem),
        "ts": now.isoformat(timespec="seconds"),
        "cwd": payload.get("cwd", ""),
        "goal": goal[:MAX_GOAL],
        "tools": dict(tools),
        "n_tool_calls": sum(tools.values()),
        "result": result[:MAX_RESULT],
    }

    log = mem_dir / f"{now:%Y-%m}.jsonl"
    sid = record["session_id"]
    # Dedupe by session: one (latest) record per session in the month file.
    kept = []
    if log.exists():
        for line in log.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                if json.loads(line).get("session_id") == sid:
                    continue
            except json.JSONDecodeError:
                continue
            kept.append(line)
    kept.append(json.dumps(record, ensure_ascii=False))
    try:
        log.write_text("\n".join(kept) + "\n", encoding="utf-8")
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
