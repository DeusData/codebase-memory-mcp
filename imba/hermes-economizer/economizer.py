"""Spend ledger + budget + cost estimation (ported from our Hermes budget/cost).

Token usage is NOT exposed in Hermes plugin hooks, so cost here is an *estimate*:
~4 chars/token on the user+assistant text, priced by a per-model table. Good
enough for a soft budget guardrail; for exact accounting use Hermes' langfuse
observability plugin.

State lives in $HERMES_HOME/economizer/ledger.jsonl (append-only JSONL).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import yaml

_PLUGIN_DIR = Path(__file__).resolve().parent

# ---- config ---------------------------------------------------------------

_DEFAULTS: dict[str, Any] = {
    "cap_usd": 5.0,
    "window": "day",          # day | session
    "warn_ratio": 0.8,
    "terse": True,             # inject token-economy baseline once per session
    "advise_routing": True,    # nudge cheaper model on simple tasks
    # prices: USD per 1M tokens, matched by substring of the model id
    "prices": {
        "haiku":  {"in": 1.0,  "out": 5.0},
        "sonnet": {"in": 3.0,  "out": 15.0},
        "opus":   {"in": 15.0, "out": 75.0},
        "gpt-4o-mini": {"in": 0.15, "out": 0.6},
        "gpt-4o": {"in": 2.5, "out": 10.0},
    },
    # which model alias is "cheap" — used only for the advisory nudge
    "cheap_hint": "haiku",
}


def hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))


def _state_dir() -> Path:
    d = hermes_home() / "economizer"
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_config() -> dict[str, Any]:
    cfg = dict(_DEFAULTS)
    # plugin-local config.yaml, then $HERMES_HOME/economizer/config.yaml override
    for path in (_PLUGIN_DIR / "config.yaml", _state_dir() / "config.yaml"):
        if path.exists():
            try:
                data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
                cfg.update(data)
            except (OSError, yaml.YAMLError):
                pass
    # env overrides (handy for testing)
    if os.environ.get("ECON_CAP_USD"):
        try:
            cfg["cap_usd"] = float(os.environ["ECON_CAP_USD"])
        except ValueError:
            pass
    return cfg


# ---- token + cost estimate ------------------------------------------------

def estimate_tokens(text: str | None) -> int:
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def _price_for(model: str, prices: dict[str, Any]) -> dict[str, float]:
    m = (model or "").lower()
    # longest key match first so "gpt-4o-mini" beats "gpt-4o"
    for key in sorted(prices, key=len, reverse=True):
        if key.lower() in m:
            return prices[key]
    return {"in": 0.0, "out": 0.0}


def estimate_cost(model: str, in_tokens: int, out_tokens: int, prices: dict[str, Any]) -> float:
    p = _price_for(model, prices)
    return in_tokens / 1_000_000 * float(p.get("in", 0)) + \
        out_tokens / 1_000_000 * float(p.get("out", 0))


# ---- ledger ---------------------------------------------------------------

def _ledger() -> Path:
    return _state_dir() / "ledger.jsonl"


def log_spend(model: str, in_tokens: int, out_tokens: int, cost_usd: float,
              session_id: str = "", note: str = "") -> None:
    rec = {"ts": int(time.time()), "event": "spend", "model": model,
           "input_tokens": in_tokens, "output_tokens": out_tokens,
           "cost_usd": round(cost_usd, 6), "session_id": session_id, "note": note}
    with _ledger().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def log_event(event: str, **fields: Any) -> None:
    rec = {"ts": int(time.time()), "event": event, **fields}
    with _ledger().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _window_start(window: str) -> int:
    if window == "day":
        n = time.localtime()
        return int(time.mktime(time.struct_time(
            (n.tm_year, n.tm_mon, n.tm_mday, 0, 0, 0, 0, 0, -1))))
    return 0


def spent(window: str = "day", session_id: str | None = None) -> float:
    p = _ledger()
    if not p.exists():
        return 0.0
    since = _window_start(window)
    total = 0.0
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("event") != "spend" or ev.get("ts", 0) < since:
            continue
        if session_id is not None and ev.get("session_id") != session_id:
            continue
        total += float(ev.get("cost_usd", 0.0))
    return round(total, 6)


def over_budget(cfg: dict[str, Any]) -> bool:
    cap = cfg.get("cap_usd")
    if not cap:
        return False
    return spent(cfg.get("window", "day")) >= float(cap)
