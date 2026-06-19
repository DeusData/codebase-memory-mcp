"""Pick the matching fablize discipline pack for a task (ported from router.sh).

Signals (smallest matching discipline only):
  - debug/error/crash      -> investigation-protocol
  - html/svg/game/chart    -> verification-grounding
  - build/create/design    -> clarify (ask-then-build)
Returns the pack text to inject as turn context, or "" if no signal.
"""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

_PACKS = Path(__file__).resolve().parent / "packs"

# Substring matching (like router.sh's *glob*) — stem-based, works for RU too
# where word boundaries (\b) fail on inflected endings (ошибк-а, падающ-ий).
_DEBUG = re.compile(r"(debug|bug|error|traceback|crash|failing|exception|"
                    r"баг|ошибк|падает|падающ|краш|трейс|стек)", re.IGNORECASE)
_RENDER = re.compile(r"(html|svg|game|canvas|chart|render|website|webpage|"
                     r"график|игр|страниц|анимац)", re.IGNORECASE)
_BUILD = re.compile(r"(build|create|make me|design|app|dashboard|feature|"
                    r"создай|сделай|построй|спроектир|приложени|фичу|дизайн)",
                    re.IGNORECASE)


@lru_cache(maxsize=8)
def _pack(name: str) -> str:
    p = _PACKS / name
    try:
        return p.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def select_discipline(task: str) -> tuple[str, str]:
    """Return (category, pack_text). category is "" when nothing matches."""
    task = task or ""
    # debug wins (most specific), then render, then build (broadest)
    if _DEBUG.search(task):
        return "investigation", _pack("investigation-protocol.txt")
    if _RENDER.search(task):
        return "verification", _pack("verification-grounding-pack.txt")
    if _BUILD.search(task):
        return "clarify", _pack("clarify-pack.txt")
    return "", ""
