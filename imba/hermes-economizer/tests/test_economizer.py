"""Unit + hook-simulation tests. No running Hermes required: we drive the hook
handlers directly with sample kwargs and a fake ctx, exactly as Hermes would."""
import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

# The plugin dir name has a hyphen, so (like Hermes itself) load it from its
# file location under a valid module name with the dir as the package search path.
PLUGIN_DIR = Path(__file__).resolve().parent.parent
_PKG = "hermes_economizer"
if _PKG not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        _PKG, PLUGIN_DIR / "__init__.py",
        submodule_search_locations=[str(PLUGIN_DIR)],
    )
    plugin = importlib.util.module_from_spec(spec)
    sys.modules[_PKG] = plugin
    spec.loader.exec_module(plugin)
else:
    plugin = sys.modules[_PKG]

econ = importlib.import_module(f"{_PKG}.economizer")
classify_complexity = importlib.import_module(f"{_PKG}.router").classify_complexity


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("ECON_CAP_USD", raising=False)
    yield


# ---- router ---------------------------------------------------------------

def test_complexity():
    assert classify_complexity("привет") == "simple"
    assert classify_complexity("отрефактори пайплайн деплоя и миграции") == "complex"
    assert classify_complexity("write a short friendly note " * 8) == "medium"


# ---- cost + ledger --------------------------------------------------------

def test_cost_estimate_by_substring():
    prices = econ._DEFAULTS["prices"]
    # "anthropic/claude-sonnet-4.6" should match "sonnet" (3/15 per 1M)
    c = econ.estimate_cost("anthropic/claude-sonnet-4.6", 1_000_000, 1_000_000, prices)
    assert round(c, 4) == 18.0
    # gpt-4o-mini must beat gpt-4o (longest match wins)
    c2 = econ.estimate_cost("openai/gpt-4o-mini", 1_000_000, 0, prices)
    assert round(c2, 4) == 0.15


def test_ledger_accrual_and_cap():
    econ.log_spend("opus", 1000, 1000, 2.0)
    econ.log_spend("opus", 1000, 1000, 2.0)
    assert econ.spent("session") == 4.0
    assert econ.over_budget({"cap_usd": 5.0, "window": "session"}) is False
    assert econ.over_budget({"cap_usd": 3.0, "window": "session"}) is True


# ---- hook simulation ------------------------------------------------------

class FakeCtx:
    def __init__(self):
        self.hooks = {}
        self.commands = {}
    def register_hook(self, name, fn):
        self.hooks.setdefault(name, []).append(fn)
    def register_command(self, name, handler=None, description=""):
        self.commands[name] = handler


def test_register_wires_all_hooks():
    ctx = FakeCtx()
    plugin.register(ctx)
    for h in ("pre_llm_call", "post_llm_call", "pre_tool_call", "on_session_end"):
        assert h in ctx.hooks
    assert "economizer" in ctx.commands


def test_pre_llm_injects_discipline_and_budget():
    out = plugin._pre_llm_call(user_message="почини падающий тест, ошибка в auth",
                               model="anthropic/claude-opus-4.8", session_id="s1")
    assert out and "context" in out
    assert "investigation" in out["context"]          # debug signal → investigation pack
    assert "бюджет" in out["context"]


def test_pre_llm_advises_cheaper_model_on_simple_task():
    out = plugin._pre_llm_call(user_message="привет", model="anthropic/claude-opus-4.8")
    assert out and "hermes model" in out["context"]   # nudge to cheap model


def test_post_llm_records_spend_and_guard_blocks():
    plugin._post_llm_call(user_message="a" * 400, assistant_response="b" * 4000,
                          model="anthropic/claude-opus-4.8", session_id="s1")
    assert econ.spent("day") > 0
    # force over budget, guardrail should block a tool call
    import os
    os.environ["ECON_CAP_USD"] = "0.0000001"
    blocked = plugin._pre_tool_call(tool_name="terminal", session_id="s1")
    assert blocked and blocked["action"] == "block"
