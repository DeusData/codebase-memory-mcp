"""hermes-economizer — cost economizer + fablize disciplines as a Hermes plugin.

Wiring (all via ctx.register_hook; never blocks the agent on error):
  pre_llm_call    -> inject matching fablize discipline + budget status into context;
                     advise a cheaper model when a simple task runs on an expensive one
  post_llm_call   -> estimate this turn's tokens/cost and append to the spend ledger
  pre_tool_call   -> block new tool calls once the budget cap is hit (the guardrail)
  on_session_end  -> write a short run summary to the ledger
Also registers a `/economizer` slash command that prints the current spend.

Drop this directory into $HERMES_HOME/plugins/hermes-economizer/ to install.
"""
from __future__ import annotations

import logging

from . import economizer as econ
from .router import classify_complexity
from .disciplines import select_discipline

logger = logging.getLogger("plugins.hermes-economizer")

_CHEAP_ALIASES = ("haiku", "mini", "flash", "lite", "small")


# ---- hook handlers --------------------------------------------------------

def _pre_llm_call(user_message: str = "", model: str = "", is_first_turn: bool = False,
                  session_id: str = "", **kwargs):
    """Inject discipline + budget status into this turn's context."""
    cfg = econ.load_config()
    parts: list[str] = []

    # 0) token-economy baseline — injected once per session (persists in history),
    #    so it shapes behavior all session at ~one-time cost. Mirrors autoclaude's
    #    Claude Code economy block, here for Hermes.
    if is_first_turn and cfg.get("terse", True):
        parts.append(
            "[hermes-economizer · economy] Экономь токены: отвечай кратко, без воды и "
            "пересказа вопроса. Читай узко — grep/head вместо целых файлов и логов "
            "(вывод инструментов висит в контексте весь разговор). Тяжёлый контекст и "
            "многошаговый анализ выноси в субагентов."
        )

    # 1) fablize discipline matching the task signal
    category, pack = select_discipline(user_message)
    if pack:
        parts.append(f"[hermes-economizer · {category}]\n{pack}")

    # 2) budget status line
    cap = cfg.get("cap_usd")
    if cap:
        used = econ.spent(cfg.get("window", "day"))
        line = f"[hermes-economizer] бюджет: ${used:.4f} / ${float(cap):.2f} за {cfg.get('window')}"
        if used >= float(cap) * float(cfg.get("warn_ratio", 0.8)):
            line += " — близко к лимиту, выбирай дешёвые шаги."
        parts.append(line)

    # 3) advisory routing: simple task on an expensive model
    if cfg.get("advise_routing", True):
        complexity = classify_complexity(user_message)
        m = (model or "").lower()
        is_cheap = any(a in m for a in _CHEAP_ALIASES)
        if complexity == "simple" and not is_cheap and m:
            parts.append(
                f"[hermes-economizer] Задача простая, а модель — {model}. "
                f"Если можно, переключись на дешёвую модель: `hermes model {cfg.get('cheap_hint','haiku')}`."
            )

    if not parts:
        return None
    return {"context": "\n\n".join(parts)}


def _post_llm_call(user_message: str = "", assistant_response: str = "", model: str = "",
                   session_id: str = "", **kwargs):
    """Estimate this turn's cost and record it."""
    cfg = econ.load_config()
    in_tok = econ.estimate_tokens(user_message)
    out_tok = econ.estimate_tokens(assistant_response)
    cost = econ.estimate_cost(model, in_tok, out_tok, cfg.get("prices", {}))
    econ.log_spend(model, in_tok, out_tok, cost, session_id=session_id, note="turn")


def _pre_tool_call(tool_name: str = "", session_id: str = "", **kwargs):
    """Budget guardrail: once over the cap, stop starting new tool work."""
    cfg = econ.load_config()
    if econ.over_budget(cfg):
        used = econ.spent(cfg.get("window", "day"))
        return {
            "action": "block",
            "message": (
                f"[hermes-economizer] Бюджет исчерпан "
                f"(${used:.2f} / ${float(cfg.get('cap_usd')):.2f} за {cfg.get('window')}). "
                "Не начинай новую работу: подведи итог сделанного, что осталось и как продолжить — и заверши."
            ),
        }
    return None


def _on_session_end(session_id: str = "", **kwargs):
    cfg = econ.load_config()
    econ.log_event("session_end", session_id=session_id,
                   spent_window=econ.spent(cfg.get("window", "day")))


def _slash_economizer(*args, **kwargs) -> str:
    cfg = econ.load_config()
    used = econ.spent(cfg.get("window", "day"))
    cap = cfg.get("cap_usd")
    cap_s = f" / ${float(cap):.2f}" if cap else ""
    return f"Hermes economizer — потрачено (оценка): ${used:.4f}{cap_s} за {cfg.get('window')}."


# ---- registration ---------------------------------------------------------

def register(ctx) -> None:
    ctx.register_hook("pre_llm_call", _pre_llm_call)
    ctx.register_hook("post_llm_call", _post_llm_call)
    ctx.register_hook("pre_tool_call", _pre_tool_call)
    ctx.register_hook("on_session_end", _on_session_end)
    try:
        ctx.register_command(
            "economizer",
            handler=_slash_economizer,
            description="Показать оценочный расход и лимит бюджета.",
        )
    except Exception:  # register_command is optional across versions
        logger.debug("register_command unavailable; slash command skipped")
