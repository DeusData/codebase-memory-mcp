#!/usr/bin/env python3
"""Roll the rank-quality campaign's runsets up into one verdict document.

Why this is separate from summarize_results.py
----------------------------------------------
`summarize_results.py` is deliberately descriptive: no confidence intervals, no
inference, nearest-rank percentiles (`:29-34`, `:2206-2210`). Those choices are correct
for a per-runset report and this file does not change them. The campaign question is
cross-runset by construction — `capability_quality` and `index_mode` are both spec-level
fields, so one corpus set at one index mode is one runset, and the comparison lives
above them.

What it answers
---------------
H1/H5  utility contamination per scorer. PR #151: "PageRank on a call graph would rank
       log.Error() and fmt.Sprintf() as the most important functions in any codebase
       ... that's not architectural importance, it's just utility popularity."
H2     whether `ORDER BY degree DESC` "gives you the same ranking signal", by rank
       correlation and top-K overlap against each score.
H4     scaffolding@K per scorer, from Tier-A labels only.
Silent drop  which directories are present under one index mode and absent under
       another, cited to the issue that reported the exclusion.

The verdicts are written so they can come out against the ranking claim. A rollup that
can only report a win is not evidence, and the counter-metrics discipline is the reason
the rest of the numbers are worth reading.

Usage:
    python3 benchmarks/rank_report.py --experiment-root /path/to/campaign --out report.md
    python3 benchmarks/rank_report.py --input a.json --input b.json --json rollup.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

try:  # Package import under tests; script-local import for direct execution.
    from benchmarks import rank_hypotheses as rank_evidence
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import rank_hypotheses as rank_evidence

# Every silent-exclusion row cites the issue that reported that directory, so the table
# is sourced evidence rather than an observation the reader has to take on trust.
# Verified against the issue text and against src/discover/discover.c.
DIRECTORY_ISSUES: dict[str, tuple[str, ...]] = {
    "scripts": ("#1406",),
    "script": ("#1406",),
    "assets": ("#1219",),
    "deploy": ("#1184",),
    "deployed": ("#1184",),
    "deployment": ("#1184",),
    "deployments": ("#1184",),
    "vendor": ("#411",),
    "docs": ("#411",),
    "doc": ("#411",),
    "examples": ("#411",),
    "e2e": ("#411",),
    "migrations": ("#411",),
    "testdata": ("#411",),
    "bin": ("#411",),
    "hack": ("#411",),
}
# The mode gate at src/discover/discover.c:448-452 applies FAST_SKIP_DIRS whenever the
# mode is not full, so full is the only reference arm a loss can be measured against.
REFERENCE_INDEX_MODE = "full"
UTILITY_CUTOFF = "10"
METRIC_ALIASES = {"leaf_hub_rate": ("utility_contamination",)}
SCAFFOLDING_CUTOFF = "10"
DEGREE_BASELINE = rank_evidence.DEGREE_BASELINE
LEGACY_DEGREE_BASELINE = "degree"
SYNTHETIC_CORPUS = "synthetic-rank-v1"


def directory_issues(name: str) -> str:
    return ", ".join(DIRECTORY_ISSUES.get(name, ())) or "unreported"


def rank_cases(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten result documents into rank-quality cases, keeping the index mode.

    index_mode lives in the document's parameters rather than the case, because it is a
    property of the whole run; carrying it down is what makes the coverage diff possible.
    """
    cases: list[dict[str, Any]] = []
    for document in documents:
        parameters = document.get("parameters") or {}
        for case in document.get("cases") or []:
            if not isinstance(case, dict) or case.get("scenario") != "rank_quality":
                continue
            corpus = case.get("corpus") or {}
            cases.append(
                {
                    **case,
                    # A canary cell has no corpus: it runs on the synthetic fixture.
                    # Naming it here keeps every table keyed the same way, and the
                    # verdicts still exclude it because it registers no hypothesis.
                    "corpus_id": corpus.get("id") or SYNTHETIC_CORPUS,
                    "language": corpus.get("language") or "unknown",
                    "index_mode": parameters.get("index_mode"),
                    "repetition": (
                        parameters.get("repetition")
                        or document.get("repetition")
                        or case.get("repetition")
                    ),
                    "config_profile": parameters.get("config_profile"),
                    "cell_name": parameters.get("cell_name"),
                    "informs_hypotheses": parameters.get("informs_hypotheses") or [],
                    "questions": parameters.get("questions") or [],
                    # The knob canary reads these; they are run parameters rather than
                    # case fields, so they are carried down once here.
                    "config_overrides": parameters.get("config_overrides") or {},
                }
            )
    return cases


def partition_usable(cases: list[dict[str, Any]]) -> tuple[list, list]:
    """Split cases into evidence and excluded, with the reason recorded.

    A cell whose ranking views were stale measured the silent degree fallback at
    src/store/store.c:11695-11711, not the score it asked for. Dropping it silently
    would overstate agreement between the scores; dropping it loudly is the point.
    """
    usable: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for case in cases:
        staleness = case.get("rank_score_staleness") or {}
        if staleness.get("available") and staleness.get("rank_views_fresh") is False:
            excluded.append(
                {
                    "corpus": case["corpus_id"],
                    "index_mode": case.get("index_mode"),
                    "reason": "rank_views_stale",
                    "detail": (
                        "search_graph falls back to a degree sort when pagerank, "
                        "linkrank or node_degree are stale, so this cell does not "
                        "measure the requested score"
                    ),
                }
            )
            continue
        usable.append(case)
    return usable, excluded


def cutoff_metric(case: dict[str, Any], cutoff: str, metric: str) -> dict[str, Any]:
    """One metric for every applicable scorer in one case, at one cutoff."""
    scorers = ((case.get("rank_score_probes") or {}).get("scorers")) or {}
    values: dict[str, Any] = {}
    for name, entry in scorers.items():
        if not isinstance(entry, dict) or not entry.get("applicable"):
            continue
        window = (entry.get("by_cutoff") or {}).get(cutoff)
        if not isinstance(window, dict):
            continue
        # utility_contamination is the pre-rename spelling of leaf_hub_rate; accepted so
        # runsets recorded before the rename still parse rather than reading as empty.
        for key in (metric, *METRIC_ALIASES.get(metric, ())):
            if key in window:
                values[name] = window[key]
                break
    return values


def scorer_table(
    cases: list[dict[str, Any]], cutoff: str, metric: str
) -> list[dict[str, Any]]:
    rows = []
    for case in cases:
        values = cutoff_metric(case, cutoff, metric)
        if not values:
            continue
        rows.append(
            {
                "corpus": case["corpus_id"],
                "language": case.get("language") or "unknown",
                "index_mode": case.get("index_mode"),
                "discriminates": (case.get("corpus") or {}).get("discriminates") or [],
                "repetition": case.get("repetition"),
                "config_profile": case.get("config_profile"),
                "config_overrides": case.get("config_overrides") or {},
                "cutoff": int(cutoff),
                "scores": values,
            }
        )
    return sorted(rows, key=lambda row: (row["corpus"], str(row["index_mode"])))


def scorer_value(scores: dict[str, Any], scorer: str) -> Any:
    """Read a canonical scorer while accepting retained pre-registry documents."""
    if scorer in scores:
        return scores[scorer]
    for alias, canonical in rank_evidence.SCORER_ALIASES.items():
        if canonical == scorer and alias in scores:
            return scores[alias]
    return None


def collapse_scorer_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse repetitions/detail cells to the independent corpus unit.

    Means summarize repeated observations; they do not create extra sample units. For R
    rows and S scorer names this is O(R*S) time and O(C*S) memory for C corpora.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["corpus"], []).append(row)
    collapsed: list[dict[str, Any]] = []
    for corpus, observations in sorted(grouped.items()):
        names = sorted(
            {name for observation in observations for name in observation["scores"]}
        )
        scores = {
            name: mean(
                [
                    observation["scores"].get(name)
                    for observation in observations
                    if isinstance(observation["scores"].get(name), (int, float))
                ]
            )
            for name in names
        }
        collapsed.append(
            {
                "corpus": corpus,
                "language": next(
                    (
                        row.get("language")
                        for row in observations
                        if row.get("language") and row.get("language") != "unknown"
                    ),
                    "unknown",
                ),
                "discriminates": sorted(
                    {
                        value
                        for row in observations
                        for value in row.get("discriminates") or []
                    }
                ),
                "observation_count": len(observations),
                "scores": scores,
            }
        )
    return collapsed


def degree_agreement(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """H2: does degree give "the same ranking signal"? Reported either way.

    Real corpora only. The canary arm measures a 17-file fixture built to have one
    structurally central symbol, where the two scores agree at Spearman 0.979 by
    construction; including it reported PR #151's claim as confirmed over 15 "corpora".
    """
    rows = []
    for case in cases:
        if case["corpus_id"] == SYNTHETIC_CORPUS:
            continue
        comparisons = (case.get("rank_score_probes") or {}).get("comparisons") or {}
        if not comparisons:
            continue
        rows.append(
            {
                "corpus": case["corpus_id"],
                "language": case.get("language") or "unknown",
                "index_mode": case.get("index_mode"),
                "repetition": case.get("repetition"),
                "comparisons": comparisons,
            }
        )
    return sorted(rows, key=lambda row: (row["corpus"], str(row["index_mode"])))


def retrieval_quality_by_scorer(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse paired graded-query sort variants to corpus/scorer evidence units."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    languages: dict[str, str] = {}
    for case in cases:
        if case["corpus_id"] == SYNTHETIC_CORPUS:
            continue
        summary = ((case.get("oracles") or {}).get("scorer_variant_quality")) or {}
        languages[case["corpus_id"]] = case.get("language") or "unknown"
        for scorer, values in summary.items():
            if isinstance(values, dict):
                grouped.setdefault((case["corpus_id"], scorer), []).append(values)
    rows = []
    for (corpus, scorer), observations in sorted(grouped.items()):
        rows.append(
            {
                "corpus": corpus,
                "language": languages.get(corpus, "unknown"),
                "scorer": scorer,
                "observation_count": len(observations),
                "graded_query_count": sum(
                    int(item.get("query_count") or 0) for item in observations
                ),
                "mean_ndcg": mean([item.get("mean_ndcg") for item in observations]),
                "mean_reciprocal_rank": mean(
                    [item.get("mean_reciprocal_rank") for item in observations]
                ),
                "hit_at_1_rate": mean(
                    [item.get("hit_at_1_rate") for item in observations]
                ),
            }
        )
    return rows


def silent_drop(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Directories present under full indexing and absent under another mode.

    Compared per corpus so a difference between corpora cannot be read as a mode
    effect. Only losses are emitted: a directory that gained files across modes is not
    a silent exclusion, and reporting it as a zero row would pad the table.
    """
    reference: dict[str, dict[str, int]] = {}
    for case in cases:
        coverage = case.get("corpus_coverage") or {}
        if case.get("index_mode") == REFERENCE_INDEX_MODE and coverage.get("available"):
            reference[case["corpus_id"]] = coverage.get("top_level_directories") or {}
    rows: list[dict[str, Any]] = []
    for case in cases:
        mode = case.get("index_mode")
        coverage = case.get("corpus_coverage") or {}
        corpus_id = case["corpus_id"]
        if mode == REFERENCE_INDEX_MODE or not coverage.get("available"):
            continue
        baseline = reference.get(corpus_id)
        if baseline is None:
            continue
        observed = coverage.get("top_level_directories") or {}
        for name, full_count in sorted(baseline.items()):
            lost = full_count - observed.get(name, 0)
            if lost <= 0:
                continue
            rows.append(
                {
                    "corpus": corpus_id,
                    "index_mode": mode,
                    "directory": name or "(repository root)",
                    "files_in_full": full_count,
                    "files_in_mode": observed.get(name, 0),
                    "files_lost": lost,
                    "issues": directory_issues(name),
                }
            )
    return rows


# The reply-detail knobs campaign_specs.py sweeps. A cell carrying none of them is the
# as-shipped default arm.
DETAIL_KEYS = ("search_limit", "trace_max_results", "snippet_max_lines")


def detail_label(overrides: dict[str, str]) -> str:
    declared = [f"{key}={overrides[key]}" for key in DETAIL_KEYS if key in overrides]
    return ", ".join(declared) or "default"


def detail_frontier(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Retrieval quality per 1k response tokens, per corpus, per reply-detail setting.

    Reply detail is a measured determinant of answer quality rather than presentation:
    upstream #1382 recorded recall falling 0.723 -> 0.525 on jackrabbit-oak purely
    because the graph arm returned less, an effect larger than any plausible re-ranking
    gain. Scoring per query alone cannot separate "ordered better" from "returned more",
    so the battery's own recorded response_token_estimate is the denominator.

    Cells with no recorded tokens are omitted rather than divided by zero, which would
    print an unbounded efficiency for a cell that measured nothing.
    """
    rows: list[dict[str, Any]] = []
    for case in cases:
        oracles = case.get("oracles") or {}
        tokens = sum(
            int(oracle["response_token_estimate"])
            for oracle in oracles.values()
            if isinstance(oracle, dict)
            and isinstance(oracle.get("response_token_estimate"), int)
        )
        quality = oracles.get("quality")
        if tokens <= 0 or not isinstance(quality, dict):
            continue
        ndcg = quality.get("mean_ndcg_at_5")
        mrr = quality.get("mean_reciprocal_rank")
        per_1k = 1000.0 / tokens
        rows.append(
            {
                "corpus": case["corpus_id"],
                "detail": detail_label(case.get("config_overrides") or {}),
                "response_tokens": tokens,
                "ndcg_at_5": ndcg,
                "mean_reciprocal_rank": mrr,
                "ndcg_per_1k_tokens": (
                    ndcg * per_1k if isinstance(ndcg, (int, float)) else None
                ),
                "mrr_per_1k_tokens": (
                    mrr * per_1k if isinstance(mrr, (int, float)) else None
                ),
            }
        )
    return sorted(rows, key=lambda row: (row["corpus"], row["detail"]))


def knob_canary(cases: list[dict[str, Any]]) -> dict[str, Any]:
    """Which ranking knobs actually moved the published scores.

    A knob whose cell produced the baseline's pagerank fingerprint changed nothing.
    That is the failure src/cli/cli.c:7160-7173 makes silent — an unowned key is
    accepted, persisted and exits 0 — so a sweep over an inert knob reports a difference
    of exactly zero and reads exactly like "this knob does not help". Any inert knob
    fails the canary, because nothing measured downstream of it is interpretable.
    """
    baseline: str | None = None
    observed: dict[str, str] = {}
    for case in cases:
        # Within the canary arm only. The rank arm's reply-detail cells also carry
        # config_overrides (search_limit, snippet_max_lines), and counting them here
        # compared a cosign fingerprint against a synthetic-fixture baseline and
        # reported search_limit as a ranking knob proved live.
        if case["corpus_id"] != SYNTHETIC_CORPUS:
            continue
        fingerprint = case.get("rank_table_fingerprint")
        overrides = case.get("config_overrides") or {}
        if not fingerprint:
            continue
        if not overrides:
            baseline = fingerprint
            continue
        for knob in overrides:
            observed[knob] = fingerprint
    if baseline is None or not observed:
        return {
            "passed": None,
            "live": [],
            "inert": [],
            "statement": "no baseline and knob cell pair was measured",
        }
    live = sorted(knob for knob, value in observed.items() if value != baseline)
    inert = sorted(knob for knob, value in observed.items() if value == baseline)
    return {
        "passed": not inert,
        "live": live,
        "inert": inert,
        "statement": (
            f"all {len(live)} ranking knobs changed the published scores"
            if not inert
            else f"{len(inert)} ranking knob(s) left the published scores byte-identical "
            f"at an extreme value: {', '.join(inert)}. Any tuning result for these is "
            "a difference of exactly zero regardless of what the knob means."
        ),
    }


def mean(values: list[float]) -> float | None:
    numeric = [float(value) for value in values if isinstance(value, (int, float))]
    return sum(numeric) / len(numeric) if numeric else None


def discriminating(rows: list[dict[str, Any]], hypothesis: str) -> tuple[list, list]:
    """Split rows by whether corpora-v1.json registered them for this hypothesis.

    The registry states which condition each corpus was chosen to expose, before any
    run. flask carries H4 and has no hub utilities at all, so folding its zero utility
    contamination into H5 would pull the mean toward zero using a corpus that cannot
    speak to the question. Honouring the pre-registration is what keeps the corpus set
    from being chosen after the fact.
    """
    included, excluded = [], []
    for row in rows:
        (
            included if hypothesis in (row.get("discriminates") or []) else excluded
        ).append(row)
    return included, excluded


def paired_means(
    rows: list[dict[str, Any]], challenger: str
) -> tuple[float | None, float | None, list[dict[str, Any]]]:
    paired = [
        row
        for row in rows
        if isinstance(scorer_value(row["scores"], DEGREE_BASELINE), (int, float))
        and isinstance(scorer_value(row["scores"], challenger), (int, float))
    ]
    return (
        mean([scorer_value(row["scores"], DEGREE_BASELINE) for row in paired]),
        mean([scorer_value(row["scores"], challenger) for row in paired]),
        paired,
    )


def corpora_with_signal(rows: list[dict[str, Any]]) -> list[str]:
    """Corpora where the metric actually moved off zero for at least one scorer.

    A corpus on which every scorer scored 0.000 did not measure anything; it only
    contributed a zero to a mean. Every defect found on the first real campaign run —
    probes ranking File nodes, the rollup reading a key the probe does not emit, the
    canary counting another arm's cells — printed a confident verdict whose backing rows
    were entirely zero. Counting signal separately from corpora is what makes that
    visible without rerunning anything.
    """
    return sorted(
        {
            row["corpus"]
            for row in rows
            if any(
                isinstance(value, (int, float)) and value != 0
                for value in row["scores"].values()
            )
        }
    )


def contamination_verdict(
    rows: list[dict[str, Any]],
    *,
    hypothesis: str,
    metric_name: str,
    cutoff: str,
    when_degree_worse: str,
    when_degree_better: str,
    when_absent: str,
) -> dict[str, Any]:
    """Compare degree against weighted PageRank on one top-K contamination metric.

    Shared by H4 (scaffolding) and H5 (utility) because the shape is identical: both ask
    whether the arm PR #151 proposed puts more of an unwanted category in the top K than
    the weighted score does. Stated as a direction rather than a pass, and both the
    corpus count and the number of corpora that produced any signal travel with it, so
    neither a one-corpus pilot nor a set of all-zero rows can read as a campaign result.
    """
    scoped_observations, other = discriminating(rows, hypothesis)
    scoped = collapse_scorer_rows(scoped_observations)
    degree_mean, pagerank_mean, paired = paired_means(scoped, "pagerank")
    with_signal = corpora_with_signal(paired)
    verdict: dict[str, Any] = {
        "hypothesis": hypothesis,
        "metric": metric_name,
        "cutoff": int(cutoff),
        "corpora_compared": len(paired),
        "observation_count": sum(row.get("observation_count", 1) for row in paired),
        "corpora_with_signal": len(with_signal),
        "signal_corpora": with_signal,
        "not_discriminating": sorted({row["corpus"] for row in other}),
        "status": "measured_descriptive" if paired else "not_run",
    }
    if degree_mean is None or pagerank_mean is None:
        return {**verdict, "supported": None, "statement": when_absent}
    verdict.update({"degree_mean": degree_mean, "pagerank_mean": pagerank_mean})
    corpora = ", ".join(sorted({row["corpus"] for row in paired}))
    margin = f"{degree_mean:.3f} vs {pagerank_mean:.3f} at K={cutoff} over {corpora}"
    if not with_signal:
        return {
            **verdict,
            "supported": None,
            "statement": (
                f"every scorer measured zero on all {len(paired)} corpora, so this "
                f"metric did not discriminate anything ({margin})"
            ),
        }
    if degree_mean == pagerank_mean:
        return {
            **verdict,
            "supported": None,
            "statement": (
                f"no measurable difference between degree and weighted PageRank "
                f"({margin})"
            ),
        }
    degree_higher = degree_mean > pagerank_mean
    return {
        **verdict,
        # Retained for old JSON readers; the report does not use it as a decision rule.
        "supported": None,
        "effect_direction": "degree_higher" if degree_higher else "pagerank_higher",
        "effect_size": degree_mean - pagerank_mean,
        "status": "measured_descriptive",
        "statement": (
            f"{when_degree_worse} ({margin}); descriptive effect, not a thresholded verdict"
            if degree_higher
            else f"{when_degree_better} ({margin}); descriptive effect, not a thresholded verdict"
        ),
    }


def utility_verdict(
    rows: list[dict[str, Any]], non_public_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """H5: measured against the public-API label, which does not use fan-in.

    PR #151's claim is that the highest-fan-in symbols are utilities, so a metric derived
    from fan-in cannot test it — that is why leaf_hub_rate was demoted to descriptive.
    The public-API label is independent of degree: Go's export rule and Python's
    underscore privacy are name-level language rules. Where neither applies the label is
    null and the corpus contributes nothing rather than a zero, so H5 reports
    not_measurable rather than a direction derived from an empty set.

    Superseded reasoning, kept because it is why this metric exists:

    PR #151's claim is that the highest-fan-in symbols are utilities. Testing it needs a
    definition of "utility" that does not itself use fan-in, and neither candidate
    qualifies: the lexical marker list is independent of fan-in but fired on one corpus
    only, and the structural leaf-hub shape is general but selects by the very quantity
    the degree scorers rank by, so it approaches 1.0 by construction and labelled
    sklearn.base.BaseEstimator and hiredis redisCommand as utilities.

    Reported as not measurable rather than computed, because a verdict from a circular
    metric is worse than none: it reads as evidence. The leaf-hub and lexical rates are
    still emitted as descriptive statistics for whoever builds the real instrument.
    """
    if not non_public_rows:
        scoped, other = discriminating(rows, "H5")
        _, _, paired = paired_means(scoped, "pagerank")
        return {
            "hypothesis": "H5",
            "metric": "non_public_rate",
            "cutoff": int(UTILITY_CUTOFF),
            "supported": None,
            "status": "not_measurable",
            "corpora_compared": len(paired),
            "corpora_with_signal": 0,
            "signal_corpora": [],
            "not_discriminating": sorted({row["corpus"] for row in other}),
            "statement": (
                "not measurable: no corpus produced a public-API verdict. The label is a "
                "name-level language rule (Go export capitalisation, Python underscore "
                "privacy) and returns null elsewhere, so C and Rust corpora cannot "
                "contribute. leaf_hub_rate is descriptive only — it selects on the same "
                "fan-in the degree scorers rank by, so it cannot adjudicate this."
            ),
        }
    return contamination_verdict(
        non_public_rows,
        hypothesis="H5",
        metric_name="non_public_rate",
        cutoff=UTILITY_CUTOFF,
        when_degree_worse=(
            "degree DESC surfaces more non-public symbols than weighted PageRank, which "
            "is the shape PR #151 described, measured without using fan-in"
        ),
        when_degree_better=(
            "degree DESC surfaces fewer non-public symbols than weighted PageRank; the "
            "PR #151 objection does not hold on this evidence"
        ),
        when_absent="no corpus registered for H5 produced both rankings with the label",
    )


def scaffolding_verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """H4: degree counts every edge equally, including TESTS edges."""
    return contamination_verdict(
        rows,
        hypothesis="H4",
        metric_name="scaffolding",
        cutoff=SCAFFOLDING_CUTOFF,
        when_degree_worse="degree surfaces more test scaffolding than weighted PageRank",
        when_degree_better="weighted PageRank surfaces more test scaffolding than degree",
        when_absent=(
            "no corpus registered for H4 carried Tier-A labels, so scaffolding@K is "
            "null rather than inferred from this server's own test predicates"
        ),
    )


def agreement_verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """H2 observations without an arbitrary equivalence cutoff.

    Repetitions and detail profiles are averaged inside each corpus first. RBO/Jaccard
    describe the returned top page; the legacy shared-row Spearman remains visible but
    cannot establish equivalence because it ignores symbols absent from one page.
    """

    def comparison(row: dict[str, Any]) -> dict[str, Any]:
        values = row.get("comparisons") or {}
        for key in (
            f"pagerank_vs_{DEGREE_BASELINE}",
            "pagerank_vs_degree",
        ):
            if isinstance(values.get(key), dict):
                return values[key]
        return {}

    observations: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        value = comparison(row)
        if value:
            observations.setdefault(row["corpus"], []).append(value)
    per_corpus = []
    for corpus, values in sorted(observations.items()):
        per_corpus.append(
            {
                "corpus": corpus,
                "observation_count": len(values),
                "rank_biased_overlap": mean(
                    [value.get("rank_biased_overlap") for value in values]
                ),
                "top_k_jaccard": mean([value.get("top_k_jaccard") for value in values]),
                "shared_spearman_rho": mean(
                    [
                        value.get("shared_spearman_rho", value.get("spearman_rho"))
                        for value in values
                    ]
                ),
            }
        )
    if not per_corpus:
        return {
            "hypothesis": "H2",
            "supported": None,
            "corpora_compared": 0,
            "observation_count": 0,
            "corpora_with_signal": 0,
            "signal_corpora": [],
            "status": "not_run",
            "statement": "no corpus produced a degree-to-pagerank correlation",
        }
    rbo = mean([row["rank_biased_overlap"] for row in per_corpus])
    jaccard = mean([row["top_k_jaccard"] for row in per_corpus])
    spearman = mean([row["shared_spearman_rho"] for row in per_corpus])
    metric_summary = ", ".join(
        f"{name}={value:.3f}"
        for name, value in (
            ("RBO", rbo),
            ("Jaccard", jaccard),
            ("shared-row Spearman", spearman),
        )
        if value is not None
    )
    corpora = [row["corpus"] for row in per_corpus]
    return {
        "hypothesis": "H2",
        "supported": None,
        "corpora_compared": len(corpora),
        "observation_count": sum(row["observation_count"] for row in per_corpus),
        "corpora_with_signal": len(corpora),
        "signal_corpora": corpora,
        "status": "measured_descriptive",
        "rank_biased_overlap_mean": rbo,
        "top_k_jaccard_mean": jaccard,
        "spearman_mean": spearman,
        "per_corpus": per_corpus,
        "statement": (
            f"measured {metric_summary or 'no numeric agreement metric'} over "
            f"{len(corpora)} independent corpora and "
            f"{sum(row['observation_count'] for row in per_corpus)} observations. "
            "No equivalence/non-inferiority margin was pre-registered, so these effect "
            "sizes are descriptive rather than a supported/refuted verdict."
        ),
    }


def hypothesis_metadata(hypothesis_id: str, verdict: dict[str, Any]) -> dict[str, Any]:
    definition = rank_evidence.HYPOTHESES[hypothesis_id]
    return {
        "hypothesis": hypothesis_id,
        "name": definition["name"],
        "question": definition["question"],
        "family": definition["family"],
        **verdict,
    }


def not_run_verdict(hypothesis_id: str, statement: str) -> dict[str, Any]:
    return hypothesis_metadata(
        hypothesis_id,
        {
            "supported": None,
            "status": "not_run",
            "corpora_compared": 0,
            "observation_count": 0,
            "corpora_with_signal": 0,
            "signal_corpora": [],
            "statement": statement,
        },
    )


def utility_family_h1(h5: dict[str, Any]) -> dict[str, Any]:
    """Represent H1 without pretending H5's shared observations are new evidence."""
    return hypothesis_metadata(
        "H1",
        {
            "supported": None,
            "status": h5.get("status", "not_run"),
            "corpora_compared": h5.get("corpora_compared", 0),
            "observation_count": h5.get("observation_count", 0),
            "corpora_with_signal": h5.get("corpora_with_signal", 0),
            "signal_corpora": h5.get("signal_corpora", []),
            "shared_evidence_with": "H5",
            "independent_evidence": False,
            "statement": (
                "H1 and H5 share the same utility/public-surface observations; this row "
                "does not count them twice. "
                + str(h5.get("statement", "No shared evidence was measured."))
            ),
        },
    )


def linkrank_capability_verdict(cases: list[dict[str, Any]]) -> dict[str, Any]:
    observed = [
        case
        for case in cases
        if ((case.get("rank_score_probes") or {}).get("edge_linkrank") or {}).get(
            "applicable"
        )
    ]
    if not observed:
        return not_run_verdict(
            "H6", "no cell emitted an applicable edge-level LinkRank ranking"
        )
    corpora = sorted({case["corpus_id"] for case in observed})
    return hypothesis_metadata(
        "H6",
        {
            "supported": None,
            "status": "instrumented_not_evaluated",
            "corpora_compared": len(corpora),
            "observation_count": len(observed),
            "corpora_with_signal": len(corpora),
            "signal_corpora": corpora,
            "statement": (
                f"{len(observed)} cells over {len(corpora)} corpora emitted ranked "
                "source-edge-target tuples. No judged edge/path task and node-only "
                "candidate-budget control were run, so H6 has instrumentation but no verdict."
            ),
        },
    )


def language_strata(cases: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, set[str]] = {}
    observations: dict[str, int] = {}
    for case in cases:
        language = str(case.get("language") or "unknown")
        grouped.setdefault(language, set()).add(case["corpus_id"])
        observations[language] = observations.get(language, 0) + 1
    return {
        language: {
            "corpora": sorted(corpora),
            "corpus_count": len(corpora),
            "observation_count": observations[language],
        }
        for language, corpora in sorted(grouped.items())
    }


def cell_question_map(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate the audit mapping carried from matrix profile to result document."""
    records: dict[str, dict[str, Any]] = {}
    for case in cases:
        name = case.get("cell_name")
        if not isinstance(name, str) or not name:
            continue
        records[name] = {
            "cell_name": name,
            "informs_hypotheses": list(case.get("informs_hypotheses") or []),
            "questions": list(case.get("questions") or []),
        }
    return [records[name] for name in sorted(records)]


def validation_gate(rollup: dict[str, Any]) -> dict[str, Any]:
    """Whether this rollup's verdicts may be quoted, and if not, exactly why.

    Not a formality. On the first complete campaign run every one of three separate
    defects in this pipeline printed a confident verdict, and each would have been
    caught here: two produced all-zero backing rows, and the third contaminated the
    canary with another arm's cells so it reported more live knobs than exist.

    A reader must be able to see the reason without rerunning anything, so each
    precondition is reported individually rather than collapsed into one boolean.
    """
    canary = rollup["knob_canary"]["passed"]
    provisional = sorted(
        name
        for name, verdict in rollup["verdicts"].items()
        if verdict.get("status") != "decision_ready"
    )
    checks = {
        "knob_canary_passed": canary,
        "no_cells_excluded": not rollup["excluded"],
        "fixture_overlay_declared": rollup["fixture_overlay"] != [],
        "provisional_verdicts": provisional,
        "decision_rule_pre_registered": False,
        "independent_unit": "corpus; repetitions and detail profiles are observations",
    }
    checks["passed"] = False
    checks["statement"] = (
        "NOT DECISION-READY — no equivalence/non-inferiority margin or inferential "
        "decision rule was pre-registered. Report the continuous effect sizes, corpus "
        "coverage, and missing arms; do not quote supported/refuted labels."
    )
    return checks


def build_rollup(documents: list[dict[str, Any]]) -> dict[str, Any]:
    """Reduce campaign result documents to tables, verdicts and a content address."""
    cases = rank_cases(documents)
    usable, excluded = partition_usable(cases)
    utility_rows = scorer_table(usable, UTILITY_CUTOFF, "leaf_hub_rate")
    non_public_rows = [
        row
        for row in scorer_table(usable, UTILITY_CUTOFF, "non_public_rate")
        if any(value is not None for value in row["scores"].values())
    ]
    scaffolding_rows = [
        row
        for row in scorer_table(usable, SCAFFOLDING_CUTOFF, "scaffolding")
        if any(value is not None for value in row["scores"].values())
    ]
    agreement_rows = degree_agreement(usable)
    retrieval_rows = retrieval_quality_by_scorer(usable)
    h2 = hypothesis_metadata("H2", agreement_verdict(agreement_rows))
    task_quality_corpora = len({row["corpus"] for row in retrieval_rows})
    h2["task_quality_corpora"] = task_quality_corpora
    h2["retrieval_quality_by_scorer"] = retrieval_rows
    if retrieval_rows:
        h2["statement"] += (
            f" Paired graded-query quality was also measured for "
            f"{len({row['scorer'] for row in retrieval_rows})} search rank modes over "
            f"{task_quality_corpora} corpora; see retrieval_quality_by_scorer."
        )
    h4 = hypothesis_metadata("H4", scaffolding_verdict(scaffolding_rows))
    h5 = hypothesis_metadata("H5", utility_verdict(utility_rows, non_public_rows))
    h6 = linkrank_capability_verdict(usable)
    rollup: dict[str, Any] = {
        "schema_version": 1,
        "corpora": sorted({case["corpus_id"] for case in usable}),
        "index_modes": sorted({str(case.get("index_mode")) for case in usable}),
        "language_strata": language_strata(usable),
        "cell_question_map": cell_question_map(usable),
        "evidence_units": {
            "independent_unit": "corpus",
            "corpus_count": len({case["corpus_id"] for case in usable}),
            "observation_count": len(usable),
            "warning": (
                "repetitions, detail profiles, and config arms are repeated observations; "
                "they are never counted as independent corpora"
            ),
        },
        "hypothesis_registry": rank_evidence.public_hypothesis_registry(),
        "leaf_hub_rate": utility_rows,
        "non_public_rate": non_public_rows,
        "scaffolding": scaffolding_rows,
        "degree_agreement": agreement_rows,
        "retrieval_quality_by_scorer": retrieval_rows,
        "silent_drop": silent_drop(usable),
        "predicted_loss_checks": [
            {
                "corpus": case["corpus_id"],
                "index_mode": case.get("index_mode"),
                **(
                    (case.get("corpus_coverage") or {}).get("predicted_loss_check")
                    or {}
                ),
            }
            for case in usable
            if (case.get("corpus_coverage") or {}).get("predicted_loss_check")
        ],
        # Validity, not decoration: an overlaid fixture changes what every corpus-scoped
        # number means, so the state travels with the numbers.
        "fixture_overlay": sorted(
            {str((case.get("fixture") or {}).get("corpus_overlay")) for case in usable}
        ),
        "detail_frontier": detail_frontier(usable),
        "excluded": excluded,
        # A validity precondition rather than a result: an inert knob makes every
        # tuning number downstream of it a difference of exactly zero.
        "knob_canary": knob_canary(usable),
        "verdicts": {
            "H1": utility_family_h1(h5),
            "H2": h2,
            "H3": not_run_verdict(
                "H3",
                "no paired rank-enabled/rank-disabled production-scale cost and task-quality arm was analyzed",
            ),
            "H4": h4,
            "H5": h5,
            "H6": h6,
            "H7": not_run_verdict(
                "H7",
                "no pre-registered default-versus-tuned held-out language/task comparison was analyzed",
            ),
        },
    }
    rollup["validation"] = validation_gate(rollup)
    payload = json.dumps(rollup, sort_keys=True, separators=(",", ":")).encode("utf-8")
    rollup["manifest"] = {
        "rollup_sha256": hashlib.sha256(payload).hexdigest(),
        "document_count": len(documents),
        "case_count": len(cases),
        "usable_case_count": len(usable),
    }
    return rollup


def format_number(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{value:.3f}"
    return "n/a"


def scorer_section(title: str, rows: list[dict[str, Any]], note: str) -> list[str]:
    if not rows:
        return [f"### {title}", "", f"No rows. {note}", ""]
    names = sorted({name for row in rows for name in row["scores"]})
    lines = [
        f"### {title}",
        "",
        "| Corpus | Mode | " + " | ".join(names) + " |",
        "|---|---|" + "---|" * len(names),
    ]
    for row in rows:
        cells = " | ".join(format_number(row["scores"].get(name)) for name in names)
        lines.append(f"| {row['corpus']} | {row['index_mode']} | {cells} |")
    lines.extend(["", note, ""])
    return lines


def render_markdown(rollup: dict[str, Any]) -> str:
    lines = [
        "# Rank-quality campaign rollup",
        "",
        (
            f"Corpora: {', '.join(rollup['corpora']) or 'none'}. "
            f"Index modes: {', '.join(rollup['index_modes']) or 'none'}. "
            f"Fixture overlay: {', '.join(rollup['fixture_overlay']) or 'n/a'}."
        ),
        "",
        f"## Validation: {'DECISION-READY' if rollup['validation']['passed'] else 'NOT DECISION-READY'}",
        "",
        rollup["validation"]["statement"],
        "",
        (
            "Continuous effects are observations. Without a pre-registered decision rule, "
            "they are not supported/refuted findings."
        ),
        "",
        "## Verdicts",
        "",
        "| Hypothesis and plain-English name | Status | Corpora | Observations | Statement |",
        "|---|---|---|---|---|",
    ]
    for key in rank_evidence.HYPOTHESES:
        verdict = rollup["verdicts"][key]
        lines.append(
            f"| {key} — {verdict['name']} | {verdict.get('status', 'unknown')} | "
            f"{verdict.get('corpora_compared', 0)} | "
            f"{verdict.get('observation_count', 0)} | {verdict['statement']} |"
        )
    canary = rollup["knob_canary"]
    state = {True: "passed", False: "FAILED", None: "not run"}[canary["passed"]]
    lines.extend(
        [
            "",
            "## Knob-efficacy canary",
            "",
            f"**{state}** — {canary['statement']}",
            "",
            f"Live: {', '.join(canary['live']) or 'none'}.",
            "",
        ]
    )
    lines.extend(["## Ranking", ""])
    if rollup["retrieval_quality_by_scorer"]:
        lines.extend(
            [
                "### Paired graded-query quality by search rank mode",
                "",
                "| Corpus | Language | Scorer | Graded queries | nDCG | MRR | Hit@1 |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for row in rollup["retrieval_quality_by_scorer"]:
            lines.append(
                f"| {row['corpus']} | {row['language']} | {row['scorer']} | "
                f"{row['graded_query_count']} | {format_number(row['mean_ndcg'])} | "
                f"{format_number(row['mean_reciprocal_rank'])} | "
                f"{format_number(row['hit_at_1_rate'])} |"
            )
        lines.extend(
            [
                "",
                (
                    "Each corpus is one evidence unit; repeated cells are averaged. These "
                    "are effect measurements, not a supported/refuted label without a "
                    "declared decision rule."
                ),
                "",
            ]
        )
    lines.extend(
        scorer_section(
            f"Leaf-hub rate @{UTILITY_CUTOFF} (descriptive, not a verdict)",
            rollup["leaf_hub_rate"],
            "Fraction of the top-K whose fan-in is above the corpus's 99th percentile "
            "with near-zero fan-out. NOT a utility count and NOT evidence about PR #151: "
            "the degree scorers rank by the same quantity this selects on.",
        )
    )
    lines.extend(
        scorer_section(
            f"Scaffolding @{SCAFFOLDING_CUTOFF}",
            rollup["scaffolding"],
            "From Tier-A labels only; a corpus with no label file is absent rather than "
            "scored zero. On jest the product is test infrastructure, so a low value "
            "there is a defect rather than a win.",
        )
    )
    if rollup["silent_drop"]:
        lines.extend(
            [
                "## Silent drop",
                "",
                "| Corpus | Mode | Directory | Files in full | Files in mode | Lost | Issues |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for row in rollup["silent_drop"]:
            lines.append(
                f"| {row['corpus']} | {row['index_mode']} | `{row['directory']}` | "
                f"{row['files_in_full']} | {row['files_in_mode']} | {row['files_lost']} | "
                f"{row['issues']} |"
            )
        lines.append("")
    if rollup["detail_frontier"]:
        lines.extend(
            [
                "## Reply-detail frontier",
                "",
                "| Corpus | Detail | Response tokens | nDCG@5 | nDCG@5 per 1k tokens |",
                "|---|---|---|---|---|",
            ]
        )
        for row in rollup["detail_frontier"]:
            lines.append(
                f"| {row['corpus']} | {row['detail']} | {row['response_tokens']} | "
                f"{format_number(row['ndcg_at_5'])} | "
                f"{format_number(row['ndcg_per_1k_tokens'])} |"
            )
        lines.extend(
            [
                "",
                (
                    "Quality per token, not per query: upstream #1382 measured recall "
                    "0.723 -> 0.525 purely from the graph arm returning less, an effect "
                    "larger than any plausible re-ranking gain."
                ),
                "",
            ]
        )
    if rollup["excluded"]:
        lines.extend(
            [
                "## Excluded from the verdicts",
                "",
                "| Corpus | Mode | Reason |",
                "|---|---|---|",
            ]
        )
        for row in rollup["excluded"]:
            lines.append(f"| {row['corpus']} | {row['index_mode']} | {row['reason']} |")
        lines.append("")
    lines.extend(
        [
            "## Manifest",
            "",
            f"- Rollup SHA-256: `{rollup['manifest']['rollup_sha256']}`",
            f"- Result documents: {rollup['manifest']['document_count']}",
            (
                f"- Rank-quality cases: {rollup['manifest']['case_count']} "
                f"({rollup['manifest']['usable_case_count']} used)"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def load_documents(roots: list[Path], inputs: list[Path]) -> list[dict[str, Any]]:
    """Read the derived report inputs run_experiments.py already materializes.

    reports/inputs/<cell_identity>-<sha12>.json is written by materialize_report_input
    (run_experiments.py:2414-2441) and carries the full result document plus its
    provenance, so the rollup reads one stable location instead of walking attempts.
    """
    paths: list[Path] = list(inputs)
    for root in roots:
        paths.extend(sorted((root / "reports" / "inputs").glob("*.json")))
        # A container run nests each measured cohort under runsets/<content-id>/.
        paths.extend(sorted(root.glob("runsets/*/reports/inputs/*.json")))
    documents = []
    for path in sorted(set(paths)):
        documents.append(json.loads(path.read_text(encoding="utf-8")))
    return documents


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--experiment-root",
        action="append",
        default=[],
        type=Path,
        help="Campaign experiment root, repeatable; reads reports/inputs/*.json.",
    )
    parser.add_argument(
        "--input", action="append", default=[], type=Path, help="One result document."
    )
    parser.add_argument("--out", type=Path, help="Markdown output path.")
    parser.add_argument("--json", dest="json_out", type=Path, help="Rollup JSON path.")
    args = parser.parse_args(argv)

    documents = load_documents(args.experiment_root, args.input)
    if not documents:
        parser.error(
            "no result documents found; pass --experiment-root pointing at a campaign "
            "root containing reports/inputs/, or --input for a single result file"
        )
    rollup = build_rollup(documents)
    text = render_markdown(rollup)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
        print(f"wrote {args.out}")
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(rollup, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"wrote {args.json_out}")
    if not args.out and not args.json_out:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
