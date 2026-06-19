"""Task complexity classification (ported from our Hermes economizer).

Hermes plugin hooks can't switch the model per turn, so here complexity is used
for *advisory* routing only: if a clearly simple task is running on an expensive
model, the plugin nudges (in context) toward a cheaper one. The regexes are the
EN/RU set from the original economizer.
"""
from __future__ import annotations

import re

_COMPLEX_RE = re.compile(
    r"```|\b(architect|migration|benchmark|optimiz|refactor|debug|deploy|"
    r"pipeline|sql|python|typescript|design|implement|feature|build)\b",
    re.IGNORECASE,
)
_COMPLEX_RU_RE = re.compile(
    r"(архитект|миграц|оптимиз|рефактор|дебаг|деплой|пайплайн|тест|"
    r"документац|поэтапн|подробно|код|реализ|построй|спроектир)",
    re.IGNORECASE,
)


def classify_complexity(task: str) -> str:
    """simple | medium | complex."""
    task = task or ""
    n = len(task)
    multiline = task.count("\n") > 5
    technical = bool(_COMPLEX_RE.search(task) or _COMPLEX_RU_RE.search(task))
    if technical or n > 1200 or multiline:
        return "complex"
    if n <= 200 and not multiline:
        return "simple"
    return "medium"
