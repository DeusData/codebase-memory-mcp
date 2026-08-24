"""Single source of truth for rank hypotheses, options, scorers, and quick arms.

The benchmark previously spread bare H identifiers, scorer SQL, and parameter lists
across the spec generator, probe, report, and prose. That made it possible for a report
to claim H1--H7 while only computing H2/H4/H5. This module owns those definitions as
data so every surface can emit the same auditable contract.

The registry contains experiment definitions, not product defaults. Intervention values
are deliberately far enough from the defaults in ``src/pagerank/pagerank.h`` to expose
wiring or convergence behavior, and each profile states the observable it is allowed to
support. A changed fingerprint is expected from semantic interventions; numerical and
lifecycle controls have different outcomes and must not be called inert merely because
their converged ranking is unchanged.
"""

from __future__ import annotations

import re
from copy import deepcopy
from pathlib import Path
from typing import Any

PERFORMANCE_SUITE_NAME = "performance-v1 — Incremental indexing cost, correctness, and capability attribution"
RANK_SUITE_NAME = (
    "rank-evidence-v2 — Node/edge ranking quality and runtime-configuration evidence"
)
UNIFIED_SUITE_NAME = (
    "codebase-memory-evidence-v1 — Unified performance and ranking assessment"
)
ROOT = Path(__file__).resolve().parents[1]
PAGERANK_HEADER = ROOT / "src" / "pagerank" / "pagerank.h"


HYPOTHESIS_FAMILIES: dict[str, dict[str, Any]] = {
    "utility_relevance": {
        "name": "Utility popularity versus architectural relevance",
        "hypotheses": ("H1", "H5"),
        "independent_unit": "corpus",
        "reason_consolidated": (
            "H1 and H5 are opposite causal readings of the same ranked utility labels; "
            "reporting them as independent evidence would double-count each corpus."
        ),
    },
    "node_order_equivalence": {
        "name": "Node ordering equivalence",
        "hypotheses": ("H2",),
        "independent_unit": "corpus",
        "reason_consolidated": "One family contains one independently testable claim.",
    },
    "cost_and_adaptation": {
        "name": "Ranking cost and runtime adaptation",
        "hypotheses": ("H3", "H7"),
        "independent_unit": "corpus by language and task stratum",
        "reason_consolidated": (
            "H3 asks whether rank cost buys task quality; H7 asks whether runtime tuning "
            "moves that same quality/latency frontier under distribution shift."
        ),
    },
    "edge_semantics": {
        "name": "Typed-edge weighting and edge-level capability",
        "hypotheses": ("H4", "H6"),
        "independent_unit": "corpus and edge-task stratum",
        "reason_consolidated": (
            "H4 compares typed and untyped aggregation; H6 checks the information lost "
            "when an edge ranking is collapsed to a node statistic."
        ),
    },
}


HYPOTHESES: dict[str, dict[str, Any]] = {
    "H1": {
        "name": "PageRank utility-hub domination",
        "family": "utility_relevance",
        "question": (
            "Does weighted PageRank surface high-fan-in utility symbols instead of "
            "task-relevant architectural entry points?"
        ),
        "required_evidence": (
            "fan-in-independent utility or architectural labels",
            "graded task relevance",
            "paired scorer windows on independently sampled corpora",
        ),
    },
    "H2": {
        "name": "Degree and PageRank ordering equivalence",
        "family": "node_order_equivalence",
        "question": (
            "Do unweighted total degree and weighted PageRank return interchangeable "
            "top-ranked node pages for real code-search tasks?"
        ),
        "required_evidence": (
            "paired complete top-K rankings",
            "top-K overlap or rank-biased overlap",
            "per-corpus task relevance",
        ),
    },
    "H3": {
        "name": "PageRank cost without task benefit",
        "family": "cost_and_adaptation",
        "question": (
            "Does computing and refreshing PageRank add indexing latency or memory "
            "without improving judged retrieval quality over the rank-disabled arm?"
        ),
        "required_evidence": (
            "paired rank-enabled and rank-disabled cells",
            "wall time and peak RSS",
            "domain-correct graded retrieval metrics",
        ),
    },
    "H4": {
        "name": "Unweighted degree ignores edge semantics",
        "family": "edge_semantics",
        "question": (
            "Does counting every incoming and outgoing edge equally mis-rank corpora "
            "whose TESTS, USAGE, IMPORTS, and control-flow edges have different meaning?"
        ),
        "required_evidence": (
            "edge-type inventory",
            "weighted and unweighted in/out/total node scorers",
            "opposite-polarity corpus controls",
        ),
    },
    "H5": {
        "name": "Raw degree promotes utility hubs",
        "family": "utility_relevance",
        "question": (
            "Does raw degree promote high-fan-in utilities ahead of independently "
            "labelled public or architectural symbols?"
        ),
        "required_evidence": (
            "fan-in-independent utility or public-surface labels",
            "paired degree and PageRank windows",
            "pre-registered independently sampled signal corpora",
        ),
    },
    "H6": {
        "name": "LinkRank preserves edge and path evidence",
        "family": "edge_semantics",
        "question": (
            "Do edge-level LinkRank results solve judged relationship or path tasks "
            "that no node-degree ordering can represent?"
        ),
        "required_evidence": (
            "ranked source-edge-target tuples",
            "edge or path task judgments",
            "node-only baseline with the same candidate budget",
        ),
    },
    "H7": {
        "name": "Runtime weights adapt across language distributions",
        "family": "cost_and_adaptation",
        "question": (
            "Can runtime edge weights improve held-out task quality across languages "
            "without unacceptable latency, memory, or regression elsewhere?"
        ),
        "required_evidence": (
            "pre-registered language and task strata",
            "paired default and tuned profiles",
            "held-out quality, latency, memory, and stability",
        ),
    },
}


EDGE_WEIGHT_OPTIONS = frozenset(
    {
        "edge_weight_async_calls",
        "edge_weight_calls",
        "edge_weight_configures",
        "edge_weight_decorates",
        "edge_weight_default",
        "edge_weight_defines",
        "edge_weight_defines_method",
        "edge_weight_http_calls",
        "edge_weight_imports",
        "edge_weight_member_of",
        "edge_weight_tests",
        "edge_weight_usage",
        "edge_weight_writes",
    }
)

# These are disjoint by construction. A test enforces that an option cannot silently be
# interpreted as both a semantic intervention and a numerical/lifecycle control.
RANK_OPTION_GROUPS: dict[str, frozenset[str]] = {
    "ranking_semantics": EDGE_WEIGHT_OPTIONS | {"pagerank_damping"},
    "numerical_convergence_controls": frozenset(
        {"pagerank_epsilon", "pagerank_max_iter"}
    ),
    "rank_lifecycle_policies": frozenset(
        {"rank_enabled", "rank_refresh", "rank_scope"}
    ),
    "retrieval_budget_controls": frozenset(
        {"search_limit", "trace_max_results", "snippet_max_lines"}
    ),
}


# Expressions are fixed source constants, never user input. Building the repetitive SQL
# from this registry keeps scorer names, directions, labels, and formulas in lockstep.
SCORER_SPECS: dict[str, dict[str, str]] = {
    "pagerank": {
        "label": "Weighted global PageRank",
        "source": "pagerank",
        "direction": "global",
        "expression": "p.rank",
    },
    "degree_weighted_in": {
        "label": "Weighted incoming edge sum",
        "source": "node_degree",
        "direction": "in",
        "expression": "d.weighted_in",
    },
    "degree_weighted_out": {
        "label": "Weighted outgoing edge sum",
        "source": "node_degree",
        "direction": "out",
        "expression": "d.weighted_out",
    },
    "degree_weighted_total": {
        "label": "Weighted total edge sum",
        "source": "node_degree",
        "direction": "total",
        "expression": "(d.weighted_in + d.weighted_out)",
    },
    "degree_unweighted_in": {
        "label": "Unweighted incoming edge count",
        "source": "node_degree",
        "direction": "in",
        "expression": "d.total_in",
    },
    "degree_unweighted_out": {
        "label": "Unweighted outgoing edge count",
        "source": "node_degree",
        "direction": "out",
        "expression": "d.total_out",
    },
    "degree_unweighted_total": {
        "label": "Unweighted total edge count",
        "source": "node_degree",
        "direction": "total",
        "expression": "(d.total_in + d.total_out)",
    },
    "calls_in": {
        "label": "Incoming direct call count",
        "source": "node_degree",
        "direction": "in",
        "expression": "d.calls_in",
    },
    "calls_out": {
        "label": "Outgoing direct call count",
        "source": "node_degree",
        "direction": "out",
        "expression": "d.calls_out",
    },
    "calls_total": {
        "label": "Total direct call count",
        "source": "node_degree",
        "direction": "total",
        "expression": "(d.calls_in + d.calls_out)",
    },
    "linkrank_in": {
        "label": "Incoming LinkRank flow sum",
        "source": "node_degree",
        "direction": "in",
        "expression": "d.linkrank_in",
    },
    "importance_pr879": {
        "label": "PR 879 node importance score",
        "source": "node_property",
        "direction": "global",
        "expression": "CAST(json_extract(n.properties,'$.importance') AS REAL)",
    },
}

# Existing result documents and consumers use these names. Emit aliases indefinitely;
# new analysis uses the explicit formula-bearing names above.
SCORER_ALIASES: dict[str, str] = {
    "weighted_in": "degree_weighted_in",
    "degree": "degree_unweighted_total",
    "in_degree": "degree_unweighted_in",
    "importance": "importance_pr879",
}
DEGREE_BASELINE = "degree_unweighted_total"


def build_rank_probe_sql(scope: str) -> dict[str, str]:
    """Return one SQL query per canonical scorer.

    Each query reads at most ``LIMIT K`` rows into Python. SQLite may scan and maintain a
    bounded top-K sort over N ranked nodes, so the database work is O(N log K) time and
    O(K) temporary state without a score index; Python retains O(K) rows per scorer.
    """
    queries: dict[str, str] = {}
    for name, scorer in SCORER_SPECS.items():
        expression = scorer["expression"]
        if scorer["source"] == "pagerank":
            source = "FROM nodes n JOIN pagerank p ON p.node_id = n.id"
            present = ""
        elif scorer["source"] == "node_degree":
            source = "FROM nodes n JOIN node_degree d ON d.node_id = n.id"
            present = ""
        else:
            source = "FROM nodes n"
            present = " AND json_extract(n.properties,'$.importance') IS NOT NULL"
        queries[name] = (
            f"SELECT n.qualified_name, n.file_path, {expression} AS s {source} "
            f"WHERE n.project = ? AND {scope}{present} "
            "ORDER BY s DESC, n.qualified_name LIMIT ?"
        )
    return queries


_NUMERIC_DEFINE = re.compile(
    r"^#define\s+([A-Z][A-Z0-9_]+)\s+([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)\s*$",
    re.MULTILINE,
)

EDGE_WEIGHT_MACRO_STEMS: dict[str, str] = {
    "edge_weight_async_calls": "ASYNC_CALLS",
    "edge_weight_calls": "CALLS",
    "edge_weight_configures": "CONFIGURES",
    "edge_weight_decorates": "DECORATES",
    "edge_weight_default": "FALLBACK",
    "edge_weight_defines": "DEFINES",
    "edge_weight_defines_method": "DEFINES_METHOD",
    "edge_weight_http_calls": "HTTP_CALLS",
    "edge_weight_imports": "IMPORTS",
    "edge_weight_member_of": "MEMBER_OF",
    "edge_weight_tests": "TESTS",
    "edge_weight_usage": "USAGE",
    "edge_weight_writes": "WRITES",
}


def parameter_macro_contracts(
    header: Path = PAGERANK_HEADER,
) -> dict[str, dict[str, str]]:
    """Read experiment values from the product's canonical macro declarations."""
    values = dict(_NUMERIC_DEFINE.findall(header.read_text(encoding="utf-8")))

    def contract(**macros: str) -> dict[str, str]:
        missing = [macro for macro in macros.values() if macro not in values]
        if missing:
            raise RuntimeError(
                f"missing PageRank parameter macros in {header}: {missing}"
            )
        result = {field: values[macro] for field, macro in macros.items()}
        result["source"] = str(header.relative_to(ROOT))
        result["macros"] = ",".join(macros.values())
        return result

    contracts = {
        "pagerank_damping": contract(
            default="CBM_PAGERANK_DAMPING",
            recommended_min="CBM_PAGERANK_DAMPING_RECOMMENDED_MIN",
            recommended_max="CBM_PAGERANK_DAMPING_RECOMMENDED_MAX",
        ),
        "pagerank_epsilon": contract(
            default="CBM_PAGERANK_EPSILON",
            recommended_min="CBM_PAGERANK_EPSILON_RECOMMENDED_MIN",
            recommended_max="CBM_PAGERANK_EPSILON_RECOMMENDED_MAX",
        ),
        "pagerank_max_iter": contract(
            default="CBM_PAGERANK_MAX_ITER",
            declared_min="CBM_PAGERANK_MAX_ITER_MIN",
        ),
    }
    for option, stem in EDGE_WEIGHT_MACRO_STEMS.items():
        contracts[option] = contract(
            default=f"CBM_PAGERANK_WEIGHT_{stem}_DEFAULT",
            recommended_min=f"CBM_PAGERANK_WEIGHT_{stem}_RECOMMENDED_MIN",
            recommended_max=f"CBM_PAGERANK_WEIGHT_{stem}_RECOMMENDED_MAX",
        )
    return contracts


def quick_hypothesis_profiles() -> list[dict[str, Any]]:
    """Build prioritized arms from declared product values, not Python magic numbers.

    The list is ordered by evidentiary value. The quick suite runs each arm once over MCP;
    it records actual elapsed time but has no total-duration target or cutoff.
    """
    parameters = parameter_macro_contracts()

    def sources(*options: str) -> list[dict[str, str]]:
        return [
            {
                "option": option,
                "source": parameters[option]["source"],
                "macros": parameters[option]["macros"],
            }
            for option in options
        ]

    equal_weight = parameters["edge_weight_calls"]["default"]
    profiles = [
        {
            "label": "baseline",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {},
            "expected_observable": "reference ranking, quality, latency, and memory",
            "parameter_sources": [],
        },
        {
            "label": "rank-disabled",
            "config_profile": "rank_disabled",
            "capabilities": {"rank_enabled": "false"},
            "config_overrides": {},
            "expected_observable": "rank rows absent and indexing cost reduced or unchanged",
            "parameter_sources": [
                {
                    "option": "rank_enabled",
                    "source": "src/cli/cli.c:CBM_CONFIG_REGISTRY",
                    "macros": "CBM_CONFIG_RANK_ENABLED",
                }
            ],
        },
        {
            "label": "equal-edge-weights",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {
                key: equal_weight for key in sorted(EDGE_WEIGHT_OPTIONS)
            },
            "expected_observable": "typed weighting ablation changes rankings when edge types differ",
            "parameter_sources": sources(*sorted(EDGE_WEIGHT_OPTIONS)),
        },
        {
            "label": "control-flow-prior",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {
                "edge_weight_calls": parameters["edge_weight_calls"]["recommended_max"],
                "edge_weight_usage": parameters["edge_weight_usage"]["recommended_min"],
                "edge_weight_tests": parameters["edge_weight_tests"]["recommended_min"],
            },
            "expected_observable": "CALLS dominate dense USAGE and TESTS edges",
            "parameter_sources": sources(
                "edge_weight_calls", "edge_weight_usage", "edge_weight_tests"
            ),
        },
        {
            "label": "shorter-propagation",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {
                "pagerank_damping": parameters["pagerank_damping"]["recommended_min"]
            },
            "expected_observable": "semantic ranking fingerprint changes toward local structure",
            "parameter_sources": sources("pagerank_damping"),
        },
        {
            "label": "loose-convergence",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {
                "pagerank_epsilon": parameters["pagerank_epsilon"]["recommended_max"]
            },
            "expected_observable": "publication succeeds with measured latency and bounded rank drift",
            "parameter_sources": sources("pagerank_epsilon"),
        },
        {
            "label": "declared-minimum-iterations",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {
                "pagerank_max_iter": parameters["pagerank_max_iter"]["declared_min"]
            },
            "expected_observable": "non-convergence is explicit and preserves the prior published generation",
            "parameter_sources": sources("pagerank_max_iter"),
        },
        {
            "label": "project-only-scope",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {"rank_scope": "project"},
            "expected_observable": "dependency symbols are excluded while project ranks remain fresh",
            "parameter_sources": [
                {
                    "option": "rank_scope",
                    "source": "src/cli/cli.c:CBM_CONFIG_REGISTRY",
                    "macros": "CBM_CONFIG_RANK_SCOPE",
                }
            ],
        },
        {
            "label": "refresh-at-publish",
            "config_profile": "candidate_native_configuration",
            "capabilities": {},
            "config_overrides": {"rank_refresh": "at_publish"},
            "expected_observable": "rank views are fresh at publication and refresh cost is recorded",
            "parameter_sources": [
                {
                    "option": "rank_refresh",
                    "source": "src/pagerank/pagerank.h",
                    "macros": "CBM_RANK_REFRESH_AT_PUBLISH",
                }
            ],
        },
    ]
    profile_hypotheses = {
        "baseline": ("H1", "H2", "H4", "H5", "H6"),
        "rank-disabled": ("H3",),
        "equal-edge-weights": ("H4",),
        "control-flow-prior": ("H4", "H7"),
        "shorter-propagation": ("H2", "H7"),
        "loose-convergence": ("H3",),
        "declared-minimum-iterations": ("H3",),
        "project-only-scope": ("H3", "H6"),
        "refresh-at-publish": ("H3",),
    }
    for priority, profile in enumerate(profiles, start=1):
        hypothesis_ids = profile_hypotheses[profile["label"]]
        profile["cell_name"] = (
            f"RANK-CELL-{priority:02d} — {profile['label'].replace('-', ' ')}"
        )
        profile["priority"] = priority
        profile["informs_hypotheses"] = list(hypothesis_ids)
        profile["questions"] = [
            {
                "id": hypothesis_id,
                "name": HYPOTHESES[hypothesis_id]["name"],
                "question": HYPOTHESES[hypothesis_id]["question"],
            }
            for hypothesis_id in hypothesis_ids
        ]
        profile["evidence_scope"] = (
            "synthetic rank fixture: establishes wiring, publication, and measured cost; "
            "real multilingual corpora are required for task-quality generalization"
        )
    return deepcopy(profiles)


def public_hypothesis_registry() -> dict[str, Any]:
    """Return a JSON-safe registry for specs, receipts, and reports."""
    return {
        "families": deepcopy(HYPOTHESIS_FAMILIES),
        "hypotheses": deepcopy(HYPOTHESES),
        "option_groups": {
            name: sorted(values) for name, values in RANK_OPTION_GROUPS.items()
        },
        "scorers": deepcopy(SCORER_SPECS),
        "scorer_aliases": dict(SCORER_ALIASES),
        "degree_baseline": DEGREE_BASELINE,
        "suite": {
            "name": RANK_SUITE_NAME,
            "runtime_policy": "measure actual duration; do not impose a suite cutoff",
        },
    }
