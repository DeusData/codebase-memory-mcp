# hermes-economizer

A **drop-in plugin** for [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)
that adds two things to the agent you already run — no fork, no core changes:

1. **Cost economizer** — keeps an estimated spend ledger, shows a budget line in
   every turn, and **blocks new tool calls once a budget cap is hit** (a guardrail).
2. **fablize work-disciplines** — when a task signals it, injects the matching
   procedure into the turn's context: *clarify-first* (build/design), *investigation
   protocol* (debug), *verification grounding* (render/UI).

It hooks the documented Hermes plugin API (`pre_llm_call`, `post_llm_call`,
`pre_tool_call`, `on_session_end`) and a `/economizer` slash command.

## Install

```bash
cp -r hermes-economizer "$HERMES_HOME/plugins/hermes-economizer"   # default HERMES_HOME=~/.hermes
cp "$HERMES_HOME/plugins/hermes-economizer/config.yaml.example" \
   "$HERMES_HOME/plugins/hermes-economizer/config.yaml"            # optional: edit caps/prices
hermes plugins        # should list "hermes-economizer"
```

User plugins under `$HERMES_HOME/plugins/<name>/` are auto-discovered by Hermes.

## Config (`config.yaml`)

```yaml
cap_usd: 5.0          # budget cap (estimate-based)
window: day           # day | session
warn_ratio: 0.8       # warn in context once this fraction of the cap is spent
advise_routing: true  # nudge a cheaper model when a simple task runs on a pricey one
cheap_hint: haiku
prices: { haiku: {in: 1.0, out: 5.0}, sonnet: {in: 3.0, out: 15.0}, opus: {in: 15.0, out: 75.0} }
```

Spend is recorded to `$HERMES_HOME/economizer/ledger.jsonl`.

## What works / what doesn't (honest)

| Capability | Status | Note |
|---|---|---|
| Inject fablize discipline by task signal | ✅ works | via `pre_llm_call` context injection |
| Budget cap → block new tool calls | ✅ works | via `pre_tool_call` veto |
| Spend ledger + budget line in context | ✅ works | per-turn |
| Cost figure | 🟡 **estimate** | plugin hooks don't expose token usage; cost ≈ chars/4 × price table. For exact accounting use Hermes' `observability/langfuse`. |
| Auto-route model by complexity | 🟡 **advisory only** | no plugin hook switches the per-turn model; the plugin *suggests* a cheaper model. True routing needs a `providers/` integration (future work). |
| fablize "early-stop" (block premature stop) | 🟡 soft | no stop-veto hook; discipline is injected via context instead. |

## Develop / test

```bash
pip install pyyaml pytest
python3 -m pytest tests/ -q     # unit + hook-simulation (no running Hermes needed)
```

The tests drive the hook handlers directly with sample kwargs (the same arguments
Hermes passes), so behaviour is verified without a provider key.

---
*Ported from the standalone Hermes-economizer experiment (`../hermes`): the model
router, cost/budget logic and fablize discipline packs now live here as a plugin.*
