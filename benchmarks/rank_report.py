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
DEGREE_BASELINE = "degree"
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
                    "index_mode": parameters.get("index_mode"),
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
                "index_mode": case.get("index_mode"),
                "discriminates": (case.get("corpus") or {}).get("discriminates") or [],
                "cutoff": int(cutoff),
                "scores": values,
            }
        )
    return sorted(rows, key=lambda row: (row["corpus"], str(row["index_mode"])))


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
                "index_mode": case.get("index_mode"),
                "comparisons": comparisons,
            }
        )
    return sorted(rows, key=lambda row: (row["corpus"], str(row["index_mode"])))


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


# Below this, two means are one measurement's worth of noise apart and the campaign has
# no direction to report. Without it a corpus where every scorer measured 0.000 printed
# "degree DESC is the cleaner arm", which is a refutation invented from an absence.
MEANINGFUL_DIFFERENCE = 1e-9

# A direction stated from fewer corpora than this is one corpus's behaviour wearing a
# campaign's clothes. Three is the smallest number that can show a pattern rather than a
# case, and the first real run had exactly one corpus carrying the utility signal.
MINIMUM_SIGNAL_CORPORA = 3


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
        (included if hypothesis in (row.get("discriminates") or []) else excluded).append(row)
    return included, excluded


def paired_means(
    rows: list[dict[str, Any]], challenger: str
) -> tuple[float | None, float | None, list[dict[str, Any]]]:
    paired = [
        row
        for row in rows
        if isinstance(row["scores"].get(DEGREE_BASELINE), (int, float))
        and isinstance(row["scores"].get(challenger), (int, float))
    ]
    return (
        mean([row["scores"][DEGREE_BASELINE] for row in paired]),
        mean([row["scores"][challenger] for row in paired]),
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
    scoped, other = discriminating(rows, hypothesis)
    degree_mean, pagerank_mean, paired = paired_means(scoped, "pagerank")
    with_signal = corpora_with_signal(paired)
    verdict: dict[str, Any] = {
        "hypothesis": hypothesis,
        "metric": metric_name,
        "cutoff": int(cutoff),
        "corpora_compared": len(paired),
        "corpora_with_signal": len(with_signal),
        "signal_corpora": with_signal,
        "not_discriminating": sorted({row["corpus"] for row in other}),
        "status": (
            "stated"
            if len(with_signal) >= MINIMUM_SIGNAL_CORPORA
            else "provisional"
        ),
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
    if abs(degree_mean - pagerank_mean) < MEANINGFUL_DIFFERENCE:
        return {
            **verdict,
            "supported": None,
            "statement": (
                f"no measurable difference between degree and weighted PageRank "
                f"({margin})"
            ),
        }
    supported = degree_mean > pagerank_mean
    caveat = (
        ""
        if verdict["status"] == "stated"
        else (
            f" — PROVISIONAL: only {len(with_signal)} of {len(paired)} corpora produced "
            f"any signal ({', '.join(with_signal)}), below the {MINIMUM_SIGNAL_CORPORA} "
            "required to state a direction"
        )
    )
    return {
        **verdict,
        "supported": supported,
        "statement": (
            f"{when_degree_worse} ({margin}){caveat}"
            if supported
            else f"{when_degree_better} ({margin}){caveat}"
        ),
    }


def utility_verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """H5: NOT MEASURABLE with any instrument this campaign currently has.

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
    scoped, other = discriminating(rows, "H5")
    _, _, paired = paired_means(scoped, "pagerank")
    return {
        "hypothesis": "H5",
        "metric": "leaf_hub_rate",
        "cutoff": int(UTILITY_CUTOFF),
        "supported": None,
        "status": "not_measurable",
        "corpora_compared": len(paired),
        "corpora_with_signal": len(corpora_with_signal(paired)),
        "signal_corpora": corpora_with_signal(paired),
        "not_discriminating": sorted({row["corpus"] for row in other}),
        "statement": (
            "not measurable: identifying a utility requires a definition independent of "
            "fan-in, and every current one is either non-general (lexical marker list) "
            "or circular with the degree scorers it would judge (leaf-hub shape). "
            "leaf_hub_rate is reported below as a descriptive statistic, not a verdict."
        ),
    }


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
    """H2: PR #151 claims degree gives "the same ranking signal" as PageRank.

    Reads `<scorer>_vs_degree.spearman_rho`, the shape run_rank_score_probes emits.
    """
    values = [
        comparison["spearman_rho"]
        for row in rows
        for name, comparison in row["comparisons"].items()
        if name == "pagerank_vs_degree"
        and isinstance(comparison, dict)
        and isinstance(comparison.get("spearman_rho"), (int, float))
    ]
    average = mean(values)
    corpora = sorted(
        {
            row["corpus"]
            for row in rows
            if isinstance(
                (row["comparisons"].get("pagerank_vs_degree") or {}).get("spearman_rho"),
                (int, float),
            )
        }
    )
    if average is None:
        return {
            "hypothesis": "H2",
            "supported": None,
            "corpora_compared": 0,
            "corpora_with_signal": 0,
            "signal_corpora": [],
            "status": "provisional",
            "statement": "no corpus produced a degree-to-pagerank correlation",
        }
    # 0.9 is the threshold at which two orderings are interchangeable for a caller who
    # only reads a top-K page; below it the two arms return materially different pages.
    supported = average >= 0.9
    status = "stated" if len(corpora) >= MINIMUM_SIGNAL_CORPORA else "provisional"
    return {
        "hypothesis": "H2",
        "supported": supported,
        "corpora_compared": len(values),
        # A correlation is a real measurement whether or not it is near zero, so signal
        # here is the distinct-corpus count rather than a non-zero test.
        "corpora_with_signal": len(corpora),
        "signal_corpora": corpora,
        "status": status,
        "spearman_mean": average,
        "statement": (
            f"degree and PageRank order the graph near-identically "
            f"(mean Spearman {average:.3f} over {len(values)} corpora), so the cheaper "
            "signal is sufficient"
            if supported
            else f"degree and PageRank produce materially different orderings "
            f"(mean Spearman {average:.3f} over {len(values)} corpora)"
        ),
    }


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
        if verdict.get("status") != "stated"
    )
    checks = {
        "knob_canary_passed": canary,
        "no_cells_excluded": not rollup["excluded"],
        "fixture_overlay_declared": rollup["fixture_overlay"] != [],
        "provisional_verdicts": provisional,
        "minimum_signal_corpora": MINIMUM_SIGNAL_CORPORA,
    }
    checks["passed"] = bool(
        canary is True and checks["no_cells_excluded"] and not provisional
    )
    checks["statement"] = (
        "every precondition passed; the verdicts above may be quoted"
        if checks["passed"]
        else "NOT VALIDATED — "
        + "; ".join(
            reason
            for reason in (
                None if canary is True else f"knob canary {canary!r} rather than passed",
                None if checks["no_cells_excluded"] else "cells were excluded",
                None
                if not provisional
                else f"provisional verdicts: {', '.join(provisional)}",
            )
            if reason
        )
    )
    return checks


def build_rollup(documents: list[dict[str, Any]]) -> dict[str, Any]:
    """Reduce campaign result documents to tables, verdicts and a content address."""
    cases = rank_cases(documents)
    usable, excluded = partition_usable(cases)
    utility_rows = scorer_table(usable, UTILITY_CUTOFF, "leaf_hub_rate")
    scaffolding_rows = [
        row
        for row in scorer_table(usable, SCAFFOLDING_CUTOFF, "scaffolding")
        if any(value is not None for value in row["scores"].values())
    ]
    agreement_rows = degree_agreement(usable)
    rollup: dict[str, Any] = {
        "schema_version": 1,
        "corpora": sorted({case["corpus_id"] for case in usable}),
        "index_modes": sorted({str(case.get("index_mode")) for case in usable}),
        "leaf_hub_rate": utility_rows,
        "scaffolding": scaffolding_rows,
        "degree_agreement": agreement_rows,
        "silent_drop": silent_drop(usable),
        "predicted_loss_checks": [
            {
                "corpus": case["corpus_id"],
                "index_mode": case.get("index_mode"),
                **((case.get("corpus_coverage") or {}).get("predicted_loss_check") or {}),
            }
            for case in usable
            if (case.get("corpus_coverage") or {}).get("predicted_loss_check")
        ],
        # Validity, not decoration: an overlaid fixture changes what every corpus-scoped
        # number means, so the state travels with the numbers.
        "fixture_overlay": sorted(
            {
                str((case.get("fixture") or {}).get("corpus_overlay"))
                for case in usable
            }
        ),
        "detail_frontier": detail_frontier(usable),
        "excluded": excluded,
        # A validity precondition rather than a result: an inert knob makes every
        # tuning number downstream of it a difference of exactly zero.
        "knob_canary": knob_canary(usable),
        "verdicts": {
            "H2": agreement_verdict(agreement_rows),
            "H4": scaffolding_verdict(scaffolding_rows),
            "H5": utility_verdict(utility_rows),
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
        f"Corpora: {', '.join(rollup['corpora']) or 'none'}. "
        f"Index modes: {', '.join(rollup['index_modes']) or 'none'}. "
        f"Fixture overlay: {', '.join(rollup['fixture_overlay']) or 'n/a'}.",
        "",
        f"## Validation: {'PASSED' if rollup['validation']['passed'] else 'NOT VALIDATED'}",
        "",
        rollup["validation"]["statement"],
        "",
        "A verdict marked **provisional** is a computed label, not a finding, and must "
        "not be quoted as one.",
        "",
        "## Verdicts",
        "",
        "| Hypothesis | Result | Status | Corpora | With signal | Statement |",
        "|---|---|---|---|---|---|",
    ]
    for key in ("H2", "H4", "H5"):
        verdict = rollup["verdicts"][key]
        result = {True: "supported", False: "refuted", None: "inconclusive"}[
            verdict["supported"]
        ]
        status = verdict.get("status", "provisional")
        marker = result if status == "stated" else f"_{result}_"
        lines.append(
            f"| {key} | {marker} | {status} | {verdict['corpora_compared']} | "
            f"{verdict.get('corpora_with_signal', 0)} | {verdict['statement']} |"
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
                "Quality per token, not per query: upstream #1382 measured recall "
                "0.723 -> 0.525 purely from the graph arm returning less, an effect "
                "larger than any plausible re-ranking gain.",
                "",
            ]
        )
    if rollup["excluded"]:
        lines.extend(["## Excluded from the verdicts", "", "| Corpus | Mode | Reason |", "|---|---|---|"])
        for row in rollup["excluded"]:
            lines.append(f"| {row['corpus']} | {row['index_mode']} | {row['reason']} |")
        lines.append("")
    lines.extend(
        [
            "## Manifest",
            "",
            f"- Rollup SHA-256: `{rollup['manifest']['rollup_sha256']}`",
            f"- Result documents: {rollup['manifest']['document_count']}",
            f"- Rank-quality cases: {rollup['manifest']['case_count']} "
            f"({rollup['manifest']['usable_case_count']} used)",
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
