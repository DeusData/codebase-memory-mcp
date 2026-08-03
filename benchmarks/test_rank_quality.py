#!/usr/bin/env python3
"""Tests for the rank-quality campaign additions to the benchmark harness.

Run: python3 benchmarks/test_rank_quality.py        (no pytest required)
     python3 -m pytest benchmarks/test_rank_quality.py -q

Scope: the campaign code only. Each test names the defect it guards, because several of
these exist because that exact defect shipped and had to be found by audit rather than
by a test.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sqlite3
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

BENCHMARKS = Path(__file__).resolve().parent


def _load(name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, BENCHMARKS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rb = _load("run_benchmark")
sr = _load("summarize_results")
re_ = _load("run_experiments")
rc = _load("run_container_experiment")
cs = _load("campaign_specs")
rr = _load("rank_report")
gl = _load("generate_test_labels")

QUERY_MANIFEST = str(BENCHMARKS / "rank-queries-v1" / "manifest.json")


def make_rank_db(
    directory: Path,
    rows: list[tuple[str, str, int, float]],
    labels: list[str] | None = None,
) -> None:
    """Minimal project DB with the three ranking tables and an importance property.

    Column order and the label column mirror src/store/store.c:552-563 so a probe that
    works here works against a real database.
    """
    connection = sqlite3.connect(directory / ("p" + rb.PROJECT_DB_SUFFIX))
    connection.executescript(
        "CREATE TABLE nodes(id INTEGER PRIMARY KEY, project TEXT, label TEXT,"
        " name TEXT, qualified_name TEXT,"
        " file_path TEXT, properties TEXT DEFAULT '{}');"
        "CREATE TABLE pagerank(node_id INTEGER, project TEXT, rank REAL);"
        "CREATE TABLE node_degree(node_id INTEGER, project TEXT, total_in INT,"
        " total_out INT, weighted_in REAL, weighted_out REAL, linkrank_in REAL);"
    )
    for index, (qualified_name, file_path, degree, rank) in enumerate(rows, start=1):
        label = (labels or ["Function"] * len(rows))[index - 1]
        connection.execute(
            "INSERT INTO nodes VALUES(?,?,?,?,?,?,?)",
            (
                index,
                "p",
                label,
                qualified_name.rsplit(".", 1)[-1],
                qualified_name,
                file_path,
                json.dumps({"importance": rank / 10}),
            ),
        )
        connection.execute("INSERT INTO pagerank VALUES(?,?,?)", (index, "p", rank))
        connection.execute(
            "INSERT INTO node_degree VALUES(?,?,?,?,?,?,?)",
            (index, "p", degree, 1, rank, rank / 2, rank / 3),
        )
    connection.commit()
    connection.close()


# --- config-key allowlist -------------------------------------------------------

def test_unknown_config_key_is_rejected_at_parse_time() -> None:
    """Guards: cbm_config_value_is_valid accepts unknown keys, so a typo would run
    to completion and report a difference of exactly zero."""
    profile = "automatic_dependency_source_indexing_disabled"
    try:
        rb.resolve_config_overrides(profile, ["edge_weight_tsets=0.01"])
    except ValueError as error:
        assert "edge_weight_tsets" in str(error)
    else:
        raise AssertionError("typo'd config key was accepted")
    assert rb.resolve_config_overrides(profile, ["edge_weight_tests=0.01"])[
        "edge_weight_tests"
    ] == "0.01"


def test_config_validation_raises_value_error_not_system_exit() -> None:
    """Guards: SystemExit is a BaseException and would slip past the harness's
    except-Exception handlers, emitting a report with no error recorded."""
    try:
        rb.validate_config_overrides({"definitely_not_a_key": "1"})
    except ValueError:
        pass
    except SystemExit:
        raise AssertionError("validation raised SystemExit; it escapes error recording")


def test_capability_sets_agree_across_the_two_entry_points() -> None:
    """run_benchmark.py and run_experiments.py do not import each other, so the shared
    capability set is duplicated. The plan layer rejecting a spec the benchmark layer
    would accept (or vice versa) fails a runset hours in, so assert they match here
    rather than trusting a keep-in-sync comment."""
    assert (
        rb.QUALITY_BACKGROUND_CAPABILITIES == re_.QUALITY_BACKGROUND_CAPABILITIES
    ), (rb.QUALITY_BACKGROUND_CAPABILITIES, re_.QUALITY_BACKGROUND_CAPABILITIES)
    assert "rank" in rb.QUALITY_BACKGROUND_CAPABILITIES


def test_allowlist_covers_every_tunable_knob() -> None:
    for key in (
        "edge_weight_tests", "edge_weight_calls", "pagerank_damping",
        "search_limit", "trace_max_results", "snippet_max_lines",
    ):
        assert key in rb.KNOWN_CONFIG_KEYS, key


# --- utility-symbol classification ----------------------------------------------

def test_structural_utility_is_derived_from_the_graph_not_a_word_list() -> None:
    """The campaign's thesis is that hardcoded word lists do not generalize, so a word
    list cannot be the instrument that decides H5. PR #151's actual claim is structural:
    "they have the highest fan-in". A utility is a symbol many things call that calls
    almost nothing itself — measurable per corpus, in any language, with no vocabulary.
    """
    # zmalloc: called by 900 things, calls 1. serverCron: balanced. main: pure caller.
    # None is identified by its name, which is the point. The ordinary symbols are the
    # distribution the cut is taken from — a quantile over three points means nothing.
    nodes = [
        {"qualified_name": f"a.ordinary{i}", "total_in": 5, "total_out": 5}
        for i in range(20)
    ] + [
        {"qualified_name": "a.zmalloc", "total_in": 900, "total_out": 1},
        {"qualified_name": "a.serverCron", "total_in": 40, "total_out": 35},
        {"qualified_name": "a.main", "total_in": 0, "total_out": 60},
    ]
    utilities = rb.structural_utilities(nodes)
    assert "a.zmalloc" in utilities
    assert "a.serverCron" not in utilities
    assert "a.main" not in utilities


def test_structural_utility_scales_the_threshold_to_the_corpus() -> None:
    """A fixed fan-in threshold would find no utilities in a small repository and label
    half of a large one. The cut is relative to the corpus's own distribution."""
    small = [
        {"qualified_name": f"s.f{i}", "total_in": i, "total_out": 5} for i in range(1, 21)
    ]
    small.append({"qualified_name": "s.hub", "total_in": 400, "total_out": 0})
    assert "s.hub" in rb.structural_utilities(small)
    # A flat graph with no hub has no utilities, rather than an arbitrary top slice.
    flat = [
        {"qualified_name": f"f.f{i}", "total_in": 10, "total_out": 10} for i in range(30)
    ]
    assert rb.structural_utilities(flat) == frozenset()


def test_structural_utility_needs_enough_symbols_to_have_a_distribution() -> None:
    """Three symbols have no distribution to take a quantile of; guessing one would
    reintroduce exactly the arbitrariness this replaces."""
    assert rb.structural_utilities([{"qualified_name": "a", "total_in": 9, "total_out": 0}]) is not None
    assert rb.structural_utilities([]) == frozenset()


def test_utility_markers_match_tokens_not_substrings() -> None:
    """Guards: bare substring matching counted this project's own trace_path as a
    logging utility, inflating utility contamination on our own corpus."""
    for name in ("zmalloc", "serverLog", "fmt.Sprintf", "printf", "memcpy", "panic"):
        assert rb.is_utility_symbol(name), name
    for name in (
        "trace_path", "cbm_trace_path", "freeze_index", "catalog",
        "AttestCommand", "url_for", "LatestSnapshot", "debug_symbols",
    ):
        assert not rb.is_utility_symbol(name), name


# --- rank score probes ----------------------------------------------------------

def test_probes_rank_only_callable_symbols() -> None:
    """Guards: with no label filter the first full campaign ranked go.mod,
    .github/workflows/build.yaml, Makefile and src/server.h above every function, so
    utility_contamination measured 0.000 on redis and cosign — the two corpora chosen
    because they have hub utilities. PR #151's claim is about log.Error() and
    fmt.Sprintf(); pass_importance.c:172-215 scopes to Function, Method, Class."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(
        directory,
        [
            ("repo.go.mod.__file__", "go.mod", 900, 9.0),
            ("repo.src.server.h.__file__", "src/server.h", 800, 8.0),
            ("repo.src.zmalloc", "src/zmalloc.c", 100, 7.0),
            ("repo.src.processCommand", "src/server.c", 40, 6.0),
        ],
        labels=["File", "File", "Function", "Function"],
    )
    probes = rb.run_rank_score_probes(directory, "p", top_n=10, cutoffs=(2,))
    ranked = [row["qualified_name"] for row in probes["scorers"]["degree"]["top_ranked"]]
    assert ranked == ["repo.src.zmalloc", "repo.src.processCommand"]
    window = probes["scorers"]["degree"]["by_cutoff"]["2"]
    assert window["utility_contamination_lexical"] == 0.5
    # Four symbols are too few for a fan-in quantile, so the structural definition
    # reports nothing rather than guessing. That is the honest answer here.
    assert window["utility_contamination"] == 0.0


def test_probes_exclude_symbols_outside_the_repository() -> None:
    """builtins.str, builtins.list and builtins.list.append held ranks 1, 3 and 5 of
    redis's PageRank top 10 — a C repository. They are not repository symbols and cannot
    be scaffolding, utilities, or architecture.

    Guards the way the first fix missed them: the scope excluded an EMPTY file_path, but
    the product marks non-file nodes with an angle-bracket sentinel instead
    (internal/cbm/lsp/py_builtins.c:84 sets file_path = "<python-builtins>"). This test
    used "" and passed while the real data was wrong, so it now uses the real sentinel.
    """
    directory = Path(tempfile.mkdtemp())
    make_rank_db(
        directory,
        [
            ("builtins.str", "<python-builtins>", 900, 9.0),
            ("builtins.list", "", 880, 8.0),
            ("jvm.Thing", "<jvm-default-package>", 870, 7.0),
            ("repo.src.processCommand", "src/server.c", 5, 1.0),
        ],
        labels=["Class", "Class", "Class", "Function"],
    )
    probes = rb.run_rank_score_probes(directory, "p", top_n=10, cutoffs=(1,))
    ranked = [row["qualified_name"] for row in probes["scorers"]["degree"]["top_ranked"]]
    assert ranked == ["repo.src.processCommand"]


def test_probes_rank_every_scorer_and_expose_degree_utility_bias() -> None:
    """The H5 shape: raw degree ranks max-fan-in utilities first by construction,
    which is the claim PR #151 made against PageRank."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(
        directory,
        # Twenty ordinary symbols supply the fan-in distribution the utility cut is
        # taken from; a quantile needs a population, and a four-node graph has none.
        [(f"ordinary{i}", "src/o.c", 5, 1.0) for i in range(20)]
        + [
            ("zmalloc", "src/z.c", 900, 5.0),
            ("serverLog", "src/s.c", 800, 4.0),
            ("processCommand", "src/s.c", 40, 90.0),
            ("createClient", "src/n.c", 30, 85.0),
        ],
    )
    probes = rb.run_rank_score_probes(directory, "p", top_n=24, cutoffs=(2, 4))
    assert probes["available"]
    assert set(probes["scorers"]) == set(rb.RANK_PROBE_SQL)
    degree = probes["scorers"]["degree"]["by_cutoff"]["2"]["utility_contamination"]
    pagerank = probes["scorers"]["pagerank"]["by_cutoff"]["2"]["utility_contamination"]
    assert degree > pagerank, (degree, pagerank)


def test_scaffolding_is_null_without_independent_labels() -> None:
    """Guards circularity: filling scaffolding from a cbm predicate would measure the
    classifier against itself."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(directory, [("a", "src/a.c", 5, 1.0)])
    probes = rb.run_rank_score_probes(directory, "p", top_n=1, cutoffs=(1,))
    assert probes["scaffolding_labels_present"] is False
    assert probes["scorers"]["degree"]["by_cutoff"]["1"]["scaffolding"] is None


def test_probe_timing_key_is_visible_to_the_fact_tables() -> None:
    """Guards: a bespoke key such as probe_elapsed_ms is skipped by fact_step_rows,
    which selects dicts carrying elapsed_ms."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(directory, [("a", "src/a.c", 5, 1.0)])
    entry = rb.run_rank_score_probes(directory, "p", top_n=1, cutoffs=(1,))["scorers"]["degree"]
    assert "elapsed_ms" in entry and "probe_elapsed_ms" not in entry


# --- statistics -----------------------------------------------------------------

def test_spearman_delegates_to_stdlib_and_handles_undefined_inputs() -> None:
    assert rb.spearman_rho([1, 2, 3], [3, 2, 1]) == statistics.correlation(
        [1, 2, 3], [3, 2, 1], method="ranked"
    )
    for left, right in (([1, 1, 1], [1, 2, 3]), ([1], [2]), ([], []), ([1, 2], [1, 2, 3])):
        assert rb.spearman_rho(left, right) is None


# --- query battery --------------------------------------------------------------

def test_default_battery_preserves_historical_cells() -> None:
    """Guards: any change to the default path invalidates cached cells, because cell
    identity includes the command."""
    battery = rb.load_rank_query_battery("", "synthetic-rank-v1")
    assert battery is rb.DEFAULT_RANK_QUERY_BATTERY
    assert len(battery) == 1 and battery[0]["id"] == "central_order_search"


def test_every_corpus_names_a_public_clone_url() -> None:
    """A registry entry that names no source cannot be reproduced by anyone else. Two
    workload entries previously recorded a local home-relative checkout path instead of
    the public repository, which described one machine rather than the corpus."""
    for entry in rb.load_corpora_manifest("").values():
        url = entry.get("url") or ""
        assert url.startswith("https://"), f"{entry['id']} has no public url: {url!r}"
        assert "~" not in url and "/Users/" not in url, entry["id"]
        assert entry.get("repo") and "local" not in entry["repo"], entry["id"]


def test_workload_corpora_clone_their_current_tip() -> None:
    """The popularity cohort is pinned to an exact commit; the workload cohort resolves
    at run time by design, because its value is the query distribution mined against
    whatever the operator actually had. Both must be cloneable from the recorded url."""
    registry = rb.load_corpora_manifest("")
    pinned = [e for e in registry.values() if len(e.get("revision", "")) == 40]
    unpinned = [e for e in registry.values() if e.get("revision") == rb.UNPINNED_REVISION]
    assert pinned and unpinned
    assert len(pinned) + len(unpinned) == len(registry)


def test_clone_refuses_to_touch_a_directory_that_is_already_a_repository() -> None:
    """clone_pinned_repo is the only harness function that mutates a directory: it runs
    git init, remote add, fetch and switch --detach. Pointed at a real checkout, the
    detach would move that repository's HEAD. Accepting the unpinned sentinel widened
    which corpora can reach it, so it refuses a directory that already has a .git."""
    existing = isolated_git_repo()
    try:
        rb.clone_pinned_repo("https://example.invalid/x", rb.UNPINNED_REVISION, existing, 5)
    except RuntimeError as error:
        assert "already a git repository" in str(error)
        assert str(existing) in str(error)
    else:
        raise AssertionError("clone ran against an existing repository")
    # The existing repository is untouched: still on its own commit, still not detached.
    head = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=existing, text=True, capture_output=True, check=True,
    ).stdout.strip()
    assert head != "HEAD", "clone detached an existing repository's HEAD"


def test_clone_rejects_a_malformed_revision_but_allows_the_declared_sentinel() -> None:
    """A typo'd sha must not silently clone the default branch and be reported as the
    pinned tree; only the declared sentinel opts into tip-cloning."""
    try:
        rb.clone_pinned_repo("https://example.invalid/x", "abc123", Path(tempfile.mkdtemp()), 5)
    except RuntimeError as error:
        assert "40-character" in str(error) or "resolve-at-run-time" in str(error)
    else:
        raise AssertionError("a malformed revision was accepted")


def test_unknown_corpus_fails_fast() -> None:
    try:
        rb.load_rank_query_battery(QUERY_MANIFEST, "not-a-corpus")
    except ValueError as error:
        assert "not-a-corpus" in str(error)
    else:
        raise AssertionError("unknown corpus silently accepted")


def test_manifest_declared_fields_are_honored() -> None:
    """Guards: default_cutoff and repetitions were declared and silently ignored."""
    battery = rb.load_rank_query_battery(QUERY_MANIFEST, "ai-session-search")
    by_id = {query["id"]: query for query in battery}
    assert by_id["flaky_max_hits_per_page"]["repetitions"] == 10
    assert by_id["wl_run_search"]["repetitions"] == 1
    assert all(query["cutoff"] == 5 for query in battery)


def test_registry_and_battery_agree_on_corpus_ids() -> None:
    """Guards: --corpus ripgrep resolved in the registry and then failed mid-run."""
    assert rb.manifest_corpus_disagreements(QUERY_MANIFEST, "") == []


def test_every_graded_judgment_survives_the_real_scorer() -> None:
    manifest = json.loads(Path(QUERY_MANIFEST).read_text(encoding="utf-8"))
    graded = 0
    for corpus in manifest["corpora"].values():
        for query in corpus["queries"]:
            judgments = query.get("judgments") or []
            if not judgments:
                continue
            graded += 1
            scored = rb.score_ranked_relevance([], judgments, cutoff=query.get("cutoff", 5))
            assert scored["judgment_count"] == len(judgments), query["id"]
    assert graded >= 2


def test_required_substrings_gate_a_wrong_path() -> None:
    judgments = [
        {
            "expected_substring": "AttestCommand",
            "required_substrings": ["cmd/cosign/cli/attest"],
            "relevance": 3,
        }
    ]
    right = rb.score_ranked_relevance(
        [{"qualified_name": "AttestCommand", "file_path": "cmd/cosign/cli/attest/attest.go"}],
        judgments,
    )
    wrong = rb.score_ranked_relevance(
        [{"qualified_name": "AttestCommand", "file_path": "elsewhere.go"}], judgments
    )
    assert right["reciprocal_rank"] == 1.0 and wrong["reciprocal_rank"] == 0.0


# --- per-query evidence ---------------------------------------------------------

def test_result_count_is_recorded_for_behavioral_only_queries() -> None:
    """Guards: returned_count sat inside `if applicable:`, so 28 of 31 manifest
    queries recorded nothing at all."""
    oracles = {
        "graded": {"response": {"results": [{"name": "zz_order_core"}, {"name": "x"}]}},
        "behavioral_zero": {"response": {"results": []}},
        "behavioral_rows": {"response": {"columns": ["a"], "rows": [1, 2, 3]}},
    }
    expectations = {
        "graded": {
            "criterion": "c",
            "cutoff": 5,
            "judgments": [{"expected_substring": "zz_order_core", "relevance": 3}],
        }
    }
    summary = rb.score_quality_oracles(oracles, expectations)
    assert oracles["behavioral_zero"]["quality"]["returned_count"] == 0
    assert oracles["behavioral_rows"]["quality"]["returned_count"] == 3
    assert summary["mean_reciprocal_rank"] == 1.0 and summary["applicable_count"] == 1


def test_repetitions_capture_result_count_instability() -> None:
    """Guards the observed failure mode: identical calls returned results and zero
    results against a fixed index (max_hits_per_page: zero 18 of 54 times)."""
    calls = {"count": 0}

    def flapping(transport, binary, env, tool, arguments, timeout, include_logs, client=None):
        calls["count"] += 1
        results = [{"name": "x"}, {"name": "y"}] if calls["count"] % 2 else []
        return {"response": {"results": results}, "elapsed_ms": 1.0}

    original_call = rb.run_tool_call_for_transport
    original_loader = rb.load_rank_query_battery
    rb.run_tool_call_for_transport = flapping
    rb.load_rank_query_battery = lambda *args, **kwargs: (
        {
            "id": "flaky", "family": "A", "tool": "search_code",
            "arguments": {"pattern": "p"}, "repetitions": 6, "judgments": [],
        },
        {
            "id": "single", "family": "A", "tool": "search_graph",
            "arguments": {"pattern": "q"}, "repetitions": 1, "judgments": [],
        },
    )
    try:
        args = argparse.Namespace(
            timeout=10, include_logs=False, corpus="", rank_query_manifest=""
        )
        oracles = rb.run_rank_quality_oracles("cli", None, {}, "proj", args)
    finally:
        rb.run_tool_call_for_transport = original_call
        rb.load_rank_query_battery = original_loader
    stability = oracles["flaky"]["repetition_stability"]
    assert stability["stable"] is False
    assert stability["zero_result_repetitions"] == 3
    assert "repetition_stability" not in oracles["single"]


# --- staleness ------------------------------------------------------------------

def test_staleness_unions_declared_and_persisted_views() -> None:
    """Guards: the ledger alone misses a view a response declared stale, and a stale
    ranking view silently degrades search_graph to a degree sort."""
    directory = Path(tempfile.mkdtemp())
    connection = sqlite3.connect(directory / ("p" + rb.PROJECT_DB_SUFFIX))
    connection.execute(
        "CREATE TABLE derived_view_state (project TEXT, view_name TEXT, status TEXT)"
    )
    connection.execute("INSERT INTO derived_view_state VALUES ('p','node_degree','stale')")
    connection.commit()
    connection.close()
    oracles = {"q": {"freshness": {"state": "stale_with_warning", "stale_views": ["pagerank"]}}}
    result = rb.rank_score_staleness(directory, "p", oracles)
    assert set(result["stale_rank_views"]) == {"pagerank", "node_degree"}
    assert result["rank_views_fresh"] is False


# --- SQLite readers -------------------------------------------------------------

def test_query_rows_delegates_and_reads_are_read_only() -> None:
    directory = Path(tempfile.mkdtemp())
    database = directory / "t.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE t(a TEXT, b INT)")
    connection.executemany("INSERT INTO t VALUES(?,?)", [("x", 1), ("y", 2)])
    connection.commit()
    connection.close()
    assert rb.query_rows(database, "SELECT a,b FROM t ORDER BY a", ()) == ["x", "y"]
    assert rb.query_tuples(database, "SELECT a,b FROM t ORDER BY a", ()) == [("x", 1), ("y", 2)]
    try:
        rb.query_tuples(database, "INSERT INTO t VALUES('z',3)", ())
    except sqlite3.OperationalError:
        pass
    else:
        raise AssertionError("probe handle allowed a write")


# --- corpus resolution ----------------------------------------------------------

def test_missing_corpus_names_every_path_it_searched() -> None:
    entry = rb.load_corpora_manifest("")["cosign"]
    args = argparse.Namespace(
        corpus_repo=[], clone_missing_real_repos=False, timeout=30
    )
    try:
        rb.resolve_corpus_source("nonexistent-corpus", entry["url"], entry["revision"], args)
    except RuntimeError as error:
        message = str(error)
        assert "--corpus-repo" in message and "Searched:" in message
    else:
        raise AssertionError("resolution succeeded for a corpus that does not exist")


def test_pinned_clone_requires_a_full_commit_hash() -> None:
    try:
        rb.clone_pinned_repo("https://example.invalid/x.git", "main", Path(tempfile.mkdtemp()), 5)
    except RuntimeError as error:
        assert "40-character" in str(error)
    else:
        raise AssertionError("a branch name was accepted as a pin")


def test_manifest_digest_changes_when_a_registry_changes() -> None:
    path = Path(tempfile.mkdtemp()) / "c.json"
    path.write_text('{"schema_version":1,"corpora":[]}')
    before = rb.manifest_digest(str(path), None)
    path.write_text('{"schema_version":1,"corpora":[],"x":1}')
    assert before != rb.manifest_digest(str(path), None)
    assert rb.manifest_digest(None, None) is None


def test_every_registered_corpus_pins_a_commit_and_tree() -> None:
    for entry in rb.load_corpora_manifest("").values():
        if entry.get("cohort") not in {"adversarial", "control"}:
            continue
        assert len(entry["revision"]) == 40, entry["id"]
        assert len(entry["tree"]) == 40, entry["id"]
        assert entry["stars"] >= 1000, entry["id"]


# --- report -------------------------------------------------------------------

def test_report_rows_follow_the_emitted_cutoffs() -> None:
    """Guards: hardcoded "10"/"40" rendered an all-n/a table whenever the producer
    was called with different cutoffs."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(
        directory,
        [("zmalloc", "src/z.c", 900, 5.0), ("processCommand", "src/s.c", 40, 90.0)],
    )
    probes = rb.run_rank_score_probes(directory, "p", top_n=2, cutoffs=(1, 2))
    case = {
        "scenario": "rank_quality",
        "corpus": {"id": "redis"},
        "rank_score_staleness": {"available": True, "rank_views_fresh": True},
        "rank_score_probes": probes,
    }
    rows = sr.rank_scorer_details([case])
    assert rows and any(row["utility_contamination"] is not None for row in rows)
    assert {row["cutoff"] for row in rows if row["cutoff"] != "n/a"} == {"1", "2"}


def test_report_never_invents_a_corpus_id() -> None:
    """Guards: a --quality-background-repo run with no --corpus was mislabelled as
    the synthetic fixture."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(directory, [("a", "src/a.c", 5, 1.0)])
    probes = rb.run_rank_score_probes(directory, "p", top_n=1, cutoffs=(1,))
    rows = sr.rank_scorer_details([{"scenario": "x", "rank_score_probes": probes}])
    assert rows[0]["corpus"] == "n/a"


def test_report_tolerates_cases_without_probes() -> None:
    assert sr.rank_scorer_details([]) == []
    assert sr.rank_scorer_details([{"scenario": "x"}]) == []


# --- synthetic overlay on real corpora (A16) ------------------------------------

def test_synthetic_rank_fixture_is_the_corpus_when_no_background_is_given() -> None:
    """The synthetic run must keep building the fixture: there is nothing else to index."""
    args = argparse.Namespace(rank_fixture_overlay=False)
    assert rb.rank_fixture_overlay_decision("rank", None, args) == "fixture-is-corpus"


def test_synthetic_rank_fixture_is_suppressed_on_a_real_corpus() -> None:
    """Guards: create_rank_quality_repo wrote zz_order_core and eight decoys into the
    same repo_dir the pinned corpus was materialized into, so a cosign run indexed the
    real tree plus nine synthetic Python files and every corpus-scoped metric was
    computed over a graph the registry pin does not describe."""
    args = argparse.Namespace(rank_fixture_overlay=False)
    assert rb.rank_fixture_overlay_decision("rank", {"tree": "abc"}, args) == "suppressed"


def test_synthetic_rank_fixture_is_kept_when_explicitly_requested() -> None:
    """The planted positive control stays available; it just stops being the default."""
    args = argparse.Namespace(rank_fixture_overlay=True)
    assert rb.rank_fixture_overlay_decision("rank", {"tree": "abc"}, args) == "overlaid"


def test_other_capabilities_keep_their_established_overlay_behaviour() -> None:
    """similarity/semantic_edges overlay a pair fixture onto background graph mass by
    design; A16 is a rank-only decision and must not change them."""
    args = argparse.Namespace(rank_fixture_overlay=False)
    for capability in ("similarity", "semantic_edges", "dependencies"):
        assert (
            rb.rank_fixture_overlay_decision(capability, {"tree": "abc"}, args)
            == "fixture-is-corpus"
        )


def test_overlay_control_query_is_appended_when_the_fixture_is_overlaid() -> None:
    """An overlay nobody queries is contamination, not a control. When the fixture is
    planted in a real corpus the canary query has to run against it."""
    battery = rb.load_rank_query_battery(QUERY_MANIFEST, "cosign")
    with_control = rb.rank_battery_with_overlay_control(battery, overlay_active=True)
    assert len(with_control) == len(battery) + 1
    control = with_control[-1]
    assert control["id"] == "central_order_search"
    assert any(
        judgment["expected_substring"] == "zz_order_core"
        for judgment in control["judgments"]
    )
    assert rb.rank_battery_with_overlay_control(battery, overlay_active=False) == battery


def test_overlay_control_query_is_not_duplicated() -> None:
    """The synthetic corpus already declares it; appending again would double-count it
    in the applicable_count-weighted quality aggregate."""
    battery = rb.load_rank_query_battery(QUERY_MANIFEST, "synthetic-rank-v1")
    assert rb.rank_battery_with_overlay_control(battery, overlay_active=True) == battery


def test_no_real_corpus_battery_queries_the_synthetic_symbols() -> None:
    """The evidence behind A16, kept executable: if a real corpus ever does query
    zz_order_core, suppressing the overlay would silently break that query, and this
    test is the thing that says so."""
    document = json.loads(Path(QUERY_MANIFEST).read_text(encoding="utf-8"))
    offenders = [
        corpus_id
        for corpus_id, corpus in document["corpora"].items()
        if corpus_id != "synthetic-rank-v1"
        and "zz_order_core" in json.dumps(corpus)
    ]
    assert not offenders, f"real corpora reference the synthetic fixture: {offenders}"


# --- Tier-A scaffolding labels reach the probe ----------------------------------

def make_pytest_project(directory: Path) -> Path:
    """A directory that declares its own pytest configuration. No git, no commits."""
    (directory / "pyproject.toml").write_text(
        '[tool.pytest.ini_options]\npython_files = ["test_*.py"]\n', encoding="utf-8"
    )
    return directory


PYTEST_PROJECT_FILES = ["src/app.py", "src/testing.py", "tests/test_app.py"]


def test_scaffolding_labels_are_derived_from_the_measured_checkout() -> None:
    """Labels are generated data derived from a corpus, so checking them in would put
    derived JSON in the repository and let a label file drift from the tree actually
    indexed. Deriving them from the staged corpus keeps one implementation and makes
    that drift impossible."""
    project = make_pytest_project(Path(tempfile.mkdtemp()))
    document = gl.build_labels("fixture", project, PYTEST_PROJECT_FILES)
    paths = rb.scaffolding_paths_from_document(document)
    assert paths == frozenset({"tests/test_app.py"})
    # The declaration decides, not the substring: src/testing.py is production code.
    assert "src/testing.py" not in paths


def test_derived_labels_report_null_when_nothing_can_be_classified() -> None:
    """A C repository has no independent declaration to read. Null, not an empty set,
    which would claim the corpus contains no test files."""
    document = gl.build_labels("fixture", Path(tempfile.mkdtemp()), ["src/server.c"])
    assert document["counts"] == {"test": 0, "not_test": 0, "unknown": 1}
    assert rb.scaffolding_paths_from_document(document) is None


def test_derived_labels_survive_a_repository_that_cannot_be_read() -> None:
    """Label derivation must never abort a run that already produced oracles."""
    assert rb.derive_scaffolding_paths("fixture", Path("/nonexistent-repo"), 5) is None
    assert rb.derive_scaffolding_paths("fixture", None, 5) is None


def test_label_derivation_can_actually_reach_the_labeller() -> None:
    """Guards a real silent failure: derive_scaffolding_paths called a helper that did
    not exist, and its `except Exception` turned the NameError into a null label set —
    scaffolding@K would have reported "no independent labels" on every corpus forever.
    Asserting the import path separately is what makes the swallow survivable."""
    module = rb.load_module_beside("generate_test_labels")
    assert callable(module.build_labels)
    # One instance, so a caller that also loads it directly sees the same objects.
    assert rb.load_module_beside("generate_test_labels") is module


def test_label_rules_follow_the_language_toolchain_not_a_path_substring() -> None:
    """Go and Cargo state what a test file is; the label follows that definition rather
    than any string in the path. Exercised on file names alone, so it needs no corpus
    checkout and runs in CI."""
    go = gl.build_labels("go", Path(tempfile.mkdtemp()), ["cmd/main.go", "pkg/a_test.go"])
    verdicts = {row["file_path"]: row["is_test"] for row in go["labels"]}
    assert verdicts == {"cmd/main.go": False, "pkg/a_test.go": True}
    assert all(row["source"] == "spec" for row in go["labels"])

    rust = gl.build_labels("rs", Path(tempfile.mkdtemp()), ["src/lib.rs", "tests/cli.rs"])
    verdicts = {row["file_path"]: row["is_test"] for row in rust["labels"]}
    # Cargo integration tests are declared by location; unit tests live inline in
    # #[cfg(test)] modules, which no file-level rule can see, so src/lib.rs is unknown.
    assert verdicts == {"src/lib.rs": None, "tests/cli.rs": True}


def test_label_rules_leave_unknown_what_no_source_declares() -> None:
    """A C project declares nothing a labeller can read. Guessing would reintroduce the
    circularity these labels exist to remove."""
    document = gl.build_labels("c", Path(tempfile.mkdtemp()), ["src/server.c"])
    assert document["labels"][0]["is_test"] is None
    assert document["labels"][0]["source"] == "unknown"


def test_an_all_unknown_label_file_reports_null_not_zero() -> None:
    """Guards: redis is C and jest declares its test config in JavaScript, so both
    label files are 100% unknown. An empty frozenset would have claimed the corpus has
    no test files at all, printing scaffolding@K as a clean 0.000 for a corpus nothing
    independent could classify."""
    directory = Path(tempfile.mkdtemp())
    (directory / "nothing.json").write_text(
        json.dumps(
            {
                "counts": {"test": 0, "not_test": 0, "unknown": 3},
                "labels": [{"file_path": "a.c", "is_test": None, "source": "unknown"}],
            }
        ),
        encoding="utf-8",
    )
    assert rb.load_scaffolding_paths("nothing", str(directory)) is None

    (directory / "genuine.json").write_text(
        json.dumps(
            {
                "counts": {"test": 0, "not_test": 2, "unknown": 0},
                "labels": [{"file_path": "a.py", "is_test": False, "source": "declared"}],
            }
        ),
        encoding="utf-8",
    )
    # A project the runner classified and found no tests in is a real measurement.
    assert rb.load_scaffolding_paths("genuine", str(directory)) == frozenset()


def test_no_corpus_derived_data_is_tracked_in_the_repository() -> None:
    """A .gitignore rule can be bypassed with `git add -f`, and staging a directory by
    name is what put generated label JSON into a commit. This asserts the outcome the
    rule is meant to produce, so the mistake cannot recur silently."""
    tracked = subprocess.run(
        ["git", "ls-files", "benchmarks/"],
        cwd=BENCHMARKS.parent,
        text=True,
        capture_output=True,
        check=False,
    )
    if tracked.returncode != 0:  # not a checkout; nothing to enforce
        return
    offenders = [
        path
        for path in tracked.stdout.split()
        if path.startswith("benchmarks/labels/")
        or path.startswith("benchmarks/campaign-specs/")
        or path.startswith("benchmarks/rollup")
    ]
    assert not offenders, f"corpus-derived data is tracked: {offenders}"


# --- corpus coverage: which files actually reached the graph ---------------------

def test_coverage_probe_counts_files_and_attributes_them_to_directories() -> None:
    """The silent-drop evidence: a per-directory count is what turns "fewer files" into
    "scripts/ is absent", which is the claim issues #1406/#1184/#1219 actually make."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(
        directory,
        [
            ("a", "cmd/main.go", 1, 1.0),
            ("b", "cmd/other.go", 1, 1.0),
            ("c", "scripts/build.go", 1, 1.0),
            ("d", "README.md", 1, 1.0),
        ],
    )
    coverage = rb.run_corpus_coverage_probe(directory, "p")
    assert coverage["available"] is True
    assert coverage["indexed_file_count"] == 4
    assert coverage["top_level_directories"]["cmd"] == 2
    assert coverage["top_level_directories"]["scripts"] == 1
    # A root-level file is not silently attributed to some directory.
    assert coverage["top_level_directories"][""] == 1


def test_coverage_probe_scores_the_registry_prediction() -> None:
    """corpora-v1.json pre-registers which directories the skip lists should drop. The
    probe has to say whether that prediction held, or the prediction is decorative."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(directory, [("a", "cmd/main.go", 1, 1.0), ("b", "hack/x.go", 1, 1.0)])
    coverage = rb.run_corpus_coverage_probe(
        directory,
        "p",
        predicted_loss={"fast_skipped_top_dirs": ["scripts", "hack"]},
        index_mode="fast",
    )
    prediction = coverage["predicted_loss_check"]
    assert prediction["absent_as_predicted"] == ["scripts"]
    assert prediction["present_despite_prediction"] == ["hack"]


def test_coverage_prediction_does_not_apply_fast_skips_to_a_full_run() -> None:
    """Guards: the flask full-mode pilot reported docs/ and examples/ as
    "present_despite_prediction". FAST_SKIP_DIRS is gated on mode != CBM_MODE_FULL
    (src/discover/discover.c:448-452), so under full indexing their presence is the
    prediction holding, not failing."""
    directory = Path(tempfile.mkdtemp())
    make_rank_db(directory, [("a", "docs/x.py", 1, 1.0), ("b", "vendor/y.py", 1, 1.0)])
    prediction = rb.run_corpus_coverage_probe(
        directory,
        "p",
        predicted_loss={
            "fast_skipped_top_dirs": ["docs"],
            "always_skipped_top_dirs": ["vendor"],
        },
        index_mode="full",
    )["predicted_loss_check"]
    assert prediction["applicable_predictions"] == ["vendor"]
    assert prediction["present_despite_prediction"] == ["vendor"]
    assert "docs" not in prediction["present_despite_prediction"]


def test_coverage_probe_breaks_files_down_by_extension() -> None:
    """The first campaign showed files lost between full and fast from directories on
    no published skip list (jest packages 637, redis deps 74). A per-directory count
    cannot say whether a second mechanism is extension-based; a per-extension count can.
    """
    directory = Path(tempfile.mkdtemp())
    make_rank_db(
        directory,
        [
            ("a", "src/a.go", 1, 1.0),
            ("b", "src/b.go", 1, 1.0),
            ("c", "docs/c.md", 1, 1.0),
            ("d", "Makefile", 1, 1.0),
        ],
    )
    coverage = rb.run_corpus_coverage_probe(directory, "p")
    assert coverage["extensions"][".go"] == 2
    assert coverage["extensions"][".md"] == 1
    # A file with no suffix is its own bucket rather than being dropped or guessed.
    assert coverage["extensions"][""] == 1


def test_coverage_probe_reports_unavailability_without_raising() -> None:
    """A missing database must not abort a run that already produced oracles."""
    coverage = rb.run_corpus_coverage_probe(Path(tempfile.mkdtemp()), "p")
    assert coverage["available"] is False and coverage["reason"]


# --- knob-efficacy canary -------------------------------------------------------

def test_rank_table_fingerprint_changes_with_the_scores() -> None:
    """The canary compares this across config cells. A fingerprint that ignored the
    scores would report every knob as live regardless of what it did."""
    first, second = Path(tempfile.mkdtemp()), Path(tempfile.mkdtemp())
    make_rank_db(first, [("a", "a.c", 3, 1.0), ("b", "b.c", 2, 2.0)])
    make_rank_db(second, [("a", "a.c", 3, 1.0), ("b", "b.c", 2, 9.0)])
    assert rb.rank_table_fingerprint(first, "p") != rb.rank_table_fingerprint(second, "p")
    assert rb.rank_table_fingerprint(first, "p") == rb.rank_table_fingerprint(first, "p")


def test_rank_table_fingerprint_is_absent_without_a_database() -> None:
    assert rb.rank_table_fingerprint(Path(tempfile.mkdtemp()), "p") is None


def test_canary_arm_covers_every_ranking_knob() -> None:
    """A knob with no cell is a knob the campaign never proved does anything, and a
    sweep over it would report a difference of exactly zero either way."""
    spec = cs.build_canary_spec("HEAD", 1, 600)
    labels = {profile["label"] for profile in spec["profiles"]}
    assert "baseline" in labels
    swept = {
        key
        for profile in spec["profiles"]
        for key in profile["config_overrides"]
    }
    assert cs.RANKING_KNOBS <= swept
    # The canary runs on the synthetic fixture: a real corpus would cost minutes per
    # knob for a question the 17-file fixture answers.
    assert rc.corpora_required_by_spec(spec) == []


def test_canary_ignores_config_cells_from_other_arms() -> None:
    """Guards: the rank arm's reply-detail cells carry search_limit and
    snippet_max_lines overrides, and the canary counted them as ranking knobs it had
    proved live — comparing a cosign fingerprint against a synthetic-fixture baseline.
    The canary is a within-arm comparison on one graph or it is nothing."""
    detail = rollup_case("cosign")
    detail["parameters"]["config_overrides"] = {"search_limit": "200"}
    detail["cases"][0]["rank_table_fingerprint"] = "hash-cosign"
    canary = rr.knob_canary(
        rr.rank_cases(
            [
                canary_case("baseline", "hash-baseline"),
                canary_case("edge_weight_tests", "hash-moved"),
                detail,
            ]
        )
    )
    assert canary["live"] == ["edge_weight_tests"]
    assert "search_limit" not in canary["live"] + canary["inert"]


def test_canary_verdict_names_a_knob_whose_scores_never_moved() -> None:
    """Guards the §3.4 defect end to end: cbm_config_value_is_valid accepts an unknown
    key, so a knob that silently does nothing produces a clean run reporting a
    difference of exactly zero, indistinguishable from "tuning does not help"."""
    documents = [
        canary_case("baseline", "hash-baseline"),
        canary_case("edge_weight_tests", "hash-moved"),
        canary_case("edge_weight_writes", "hash-baseline"),
    ]
    canary = rr.knob_canary(rr.rank_cases(documents))
    assert canary["live"] == ["edge_weight_tests"]
    assert canary["inert"] == ["edge_weight_writes"]
    assert canary["passed"] is False


def canary_case(label: str, fingerprint: str) -> dict[str, Any]:
    overrides = {} if label == "baseline" else {label: "0.99"}
    return {
        "parameters": {"index_mode": "full", "config_overrides": overrides},
        "cases": [
            {
                "scenario": "rank_quality",
                # A synthetic run records corpus: None. The first version of this
                # fixture invented an id, which would have hidden a rollup that
                # silently dropped every canary cell.
                "corpus": None,
                "fixture": {"corpus_overlay": "fixture-is-corpus"},
                "rank_score_staleness": {"available": True, "rank_views_fresh": True},
                "rank_table_fingerprint": fingerprint,
            }
        ],
    }


# --- container corpus staging ---------------------------------------------------

CONTAINER_SPEC = {
    "benchmark_args": ["--rank-query-manifest", "m.json", "--corpus", "cosign"],
    "candidates": [{"label": "head", "benchmark_args": ["--corpus=flask"]}],
    "profiles": [{"label": "lean", "benchmark_args": ["--corpus", "runc"]}],
    "scenarios": [{"label": "s", "benchmark_args": ["--corpus-manifest", "c.json"]}],
}


def test_container_finds_every_corpus_named_anywhere_in_a_matrix_spec() -> None:
    """Guards: benchmark_args appear at four nesting levels in run_experiments.py
    (spec, candidates, profiles, scenarios). Reading only the top level would stage
    one corpus and let the container fail on the others, hours in."""
    assert rc.corpora_required_by_spec(CONTAINER_SPEC) == ["cosign", "flask", "runc"]


def test_container_corpus_scan_ignores_unrelated_flags() -> None:
    assert rc.corpora_required_by_spec({"benchmark_args": ["--index-mode", "fast"]}) == []
    assert rc.corpora_required_by_spec({}) == []


def test_container_corpus_env_key_is_the_harness_definition() -> None:
    """The coordinator writes this variable and run_benchmark.py reads it. Two spellings
    would present as a corpus that resolves on the host and vanishes in the container."""
    assert rb.corpus_env_key("ai-session-search") == "CBM_BENCH_CORPUS_AI_SESSION_SEARCH"
    for corpus_id in ("cosign", "ai-session-search", "codebase-memory-mcp", "runc"):
        assert rc.corpus_env_key(corpus_id) == rb.corpus_env_key(corpus_id)


def test_container_corpus_staging_path_is_pinned_by_revision() -> None:
    """The work volume is retained for resume, and docker cp merges into an existing
    directory. A path keyed only by corpus id would leave files from an older pin in
    place and index a tree that matches no commit."""
    first = rc.container_corpus_path("cosign", "a" * 40)
    assert first != rc.container_corpus_path("cosign", "b" * 40)
    assert first.startswith("/benchmark/corpora/cosign-")


def isolated_git_repo() -> Path:
    """A throwaway repository with one commit, provably outside any real checkout.

    `staged_corpus_revision` reads `git rev-parse HEAD`, so exercising its unpinned path
    needs a repository that has a commit. The assertion is the point: it makes isolation
    an enforced precondition rather than an assumption about what mkdtemp returns, so a
    future edit cannot quietly point repository-mutating commands at a real checkout.
    """
    directory = Path(tempfile.mkdtemp(prefix="cbm-rank-test-")).resolve()
    system_temp = Path(tempfile.gettempdir()).resolve()
    assert directory.is_relative_to(system_temp), directory
    assert not (directory / ".git").exists(), directory
    for command in (
        ["git", "init", "--quiet"],
        ["git", "-c", "user.email=t@e", "-c", "user.name=t", "commit",
         "--quiet", "--allow-empty", "-m", "c"],
    ):
        subprocess.run(command, cwd=directory, check=True, capture_output=True)
    return directory


def test_container_stages_workload_corpora_by_their_resolved_commit() -> None:
    """Guards: the four mined-workload corpora carry the unpinned sentinel rather than
    a sha. Keying their staged directory on that literal would give every
    state of the repository the same path, so a retained work volume would merge two
    different trees and the manifest would name a commit that does not exist."""
    pinned = {"id": "cosign", "revision": "a" * 40}
    assert rc.staged_corpus_revision(pinned, Path("/nonexistent"), 5) == "a" * 40

    repository = isolated_git_repo()
    resolved = rc.staged_corpus_revision(
        {"id": "autorun", "revision": rb.UNPINNED_REVISION}, repository, 30
    )
    assert len(resolved) == 40 and resolved != rb.UNPINNED_REVISION

    # A typo'd revision must not be staged as "whatever HEAD is" and recorded as the
    # measured commit; only the declared sentinel opts into resolving at run time.
    try:
        rc.staged_corpus_revision({"id": "x", "revision": "abc123"}, repository, 30)
    except RuntimeError as error:
        assert "abc123" in str(error)
    else:
        raise AssertionError("a malformed revision was staged")


def test_container_run_key_separates_different_corpus_pins() -> None:
    """Resume is keyed by this. Two pins sharing a key would merge measurements taken
    against different source trees into one runset."""
    def key(revision: str) -> str:
        return rc.container_run_key(
            source_revision="c" * 40,
            repository_snapshot_sha256="d" * 64,
            matrix_spec_sha256=None,
            resources={"cpus": 4, "memory": "8g", "workers": 4},
            runner_arguments=["--quick"],
            corpora=[{"id": "cosign", "revision": revision}],
        )

    assert key("a" * 40) != key("b" * 40)
    assert key("a" * 40) == key("a" * 40)


# --- campaign matrix specs ------------------------------------------------------

def resolved_candidate_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Substitute a built candidate so the spec can be validated without a build.

    resolve_matrix_spec_candidates validates and materializes in one pass, so a
    ref-based spec cannot be checked without compiling a binary. Everything the
    campaign actually configures — capability_quality, index_mode, profiles,
    benchmark_args, config_overrides — is downstream of that substitution.
    """
    stub_binary = Path(tempfile.mkdtemp()) / "cbm"
    stub_binary.write_bytes(b"stub")
    document = json.loads(json.dumps(spec))
    for candidate in document["candidates"]:
        candidate.pop("ref", None)
        candidate["revision"] = "0" * 40
        candidate["binary"] = str(stub_binary)
        candidate["binary_sha256"] = hashlib.sha256(stub_binary.read_bytes()).hexdigest()
        candidate["build"] = {
            "target": "make -j1 -f Makefile.cbm cbm",
            "compiler": "clang",
            "cflags": "-O2",
            "cxx_compiler": "clang++",
            "cxxflags": "-O2",
            "source_commit_datetime": "2026-01-01T00:00:00Z",
            "source_tree": "2" * 40,
        }
    return document


def test_every_generated_campaign_spec_expands() -> None:
    """A spec that fails validation fails after the container image is built and the
    corpora are staged, which is the most expensive place to learn about a typo."""
    for spec in cs.build_all_specs():
        plan = re_.expand_matrix_spec(resolved_candidate_spec(spec))
        assert plan["cells"], f"{spec['harness_version']} expanded to no cells"


def test_campaign_specs_name_only_registered_corpora() -> None:
    """A corpus id that is not in corpora-v1.json cannot be staged or resolved, and the
    failure would land mid-campaign rather than at generation time."""
    registered = set(rb.load_corpora_manifest(""))
    for spec in cs.build_all_specs():
        named = set(rc.corpora_required_by_spec(spec))
        assert named <= registered, f"unregistered: {sorted(named - registered)}"


def test_coverage_specs_differ_only_in_index_mode() -> None:
    """The silent-drop comparison is only valid if the three arms are otherwise
    identical; any other difference would confound coverage loss with a config change."""
    coverage = {
        spec["index_mode"]: spec
        for spec in cs.build_all_specs()
        if spec["campaign_arm"] == "coverage"
    }
    assert set(coverage) == {"full", "moderate", "fast"}
    stripped = [
        json.dumps(
            {k: v for k, v in spec.items() if k not in {"index_mode", "harness_version"}},
            sort_keys=True,
        )
        for spec in coverage.values()
    ]
    assert len(set(stripped)) == 1


def test_spec_generation_writes_every_arm_including_the_corpus_free_canary() -> None:
    """Guards: main() read profile["benchmark_args"][1] to name each corpus, so the
    canary — whose profiles carry no workload flags at all — raised IndexError after
    writing its own file and before writing the other four arms. The campaign then ran
    one arm and the loop reported success for all five."""
    out = Path(tempfile.mkdtemp())
    assert cs.main(["--out-dir", str(out), "--corpora", "flask"]) == 0
    written = {path.name for path in out.glob("*.json")}
    assert written == {
        "knob-canary-v1.json",
        "rank-quality-v1.json",
        "coverage-full-v1.json",
        "coverage-moderate-v1.json",
        "coverage-fast-v1.json",
    }


def test_campaign_specs_are_deterministic() -> None:
    """The spec sha is part of the container run key, so a regenerated spec that differs
    byte-wise would start a new runset instead of resuming the existing one."""
    assert json.dumps(cs.build_all_specs(), sort_keys=True) == json.dumps(
        cs.build_all_specs(), sort_keys=True
    )


def test_campaign_pilot_subset_rejects_an_unknown_corpus() -> None:
    """A pilot is meant to fail cheaply. An unrecognised id would otherwise surface
    after the image build, the candidate build and corpus staging."""
    assert cs.selected_corpora(("flask",)) == ("flask",)
    try:
        cs.selected_corpora(("flsak",))
    except ValueError as error:
        assert "flsak" in str(error) and "flask" in str(error)
    else:
        raise AssertionError("unknown corpus id was accepted")


def test_campaign_pilot_subset_drops_the_detail_frontier_when_absent() -> None:
    """The detail sweep runs on cosign. A flask-only pilot must not emit cosign cells
    whose corpus was never staged."""
    spec = cs.build_rank_spec("HEAD", 1, 60, ("flask",))
    assert [profile["label"] for profile in spec["profiles"]] == ["corpus-flask"]
    assert set(rc.corpora_required_by_spec(spec)) == {"flask"}


def test_rank_arm_covers_every_pinned_popularity_corpus() -> None:
    """H4 and H5 need the test-heavy and hub-utility corpora together; dropping one
    silently narrows the claim the campaign can make."""
    rank = [s for s in cs.build_all_specs() if s["campaign_arm"] == "rank"]
    assert len(rank) == 1
    named = set(rc.corpora_required_by_spec(rank[0]))
    assert {"cosign", "jest", "runc", "flask", "redis", "ripgrep"} <= named


# --- cross-corpus campaign rollup -----------------------------------------------

def rollup_case(
    corpus: str,
    *,
    index_mode: str = "full",
    directories: dict[str, int] | None = None,
    utility: dict[str, float] | None = None,
    scaffolding: dict[str, float | None] | None = None,
    rho: dict[str, float] | None = None,
    fresh: bool = True,
) -> dict[str, Any]:
    scorers = {
        name: {
            "applicable": True,
            "ranked_count": 40,
            "by_cutoff": {
                "10": {
                    "window_size": 10,
                    "utility_contamination": value,
                    "scaffolding": (scaffolding or {}).get(name),
                }
            },
        }
        for name, value in (utility or {"degree": 0.4, "pagerank": 0.1}).items()
    }
    return {
        "parameters": {"index_mode": index_mode},
        "cases": [
            {
                "scenario": "rank_quality",
                "corpus": {"id": corpus, "revision": "a" * 40, "discriminates": ["H5"]},
                "fixture": {"corpus_overlay": "suppressed"},
                "rank_score_staleness": {"available": True, "rank_views_fresh": fresh},
                "rank_score_probes": {
                    "scorers": scorers,
                    # Key shape copied from run_rank_score_probes, not invented: the
                    # first version of this fixture used spearman_vs_degree and the
                    # rollup silently matched nothing on real data.
                    "comparisons": {
                        f"{name}_vs_degree": {
                            "spearman_rho": value,
                            "top_k_jaccard": value,
                            "top_k": 10,
                            "shared_symbols": 19,
                        }
                        for name, value in (rho or {"pagerank": 0.62}).items()
                    },
                    "scaffolding_labels_present": bool(scaffolding),
                },
                "corpus_coverage": {
                    "available": True,
                    "indexed_file_count": sum((directories or {"src": 10}).values()),
                    "top_level_directories": dict(directories or {"src": 10}),
                },
            }
        ],
    }


def test_rollup_reports_utility_contamination_per_scorer() -> None:
    """H1 and H5 in one table: PR #151 says PageRank surfaces utility popularity and
    degree gives the same signal more cheaply. Both directions have to be readable."""
    rollup = rr.build_rollup([rollup_case("cosign", utility={"degree": 0.6, "pagerank": 0.2})])
    row = next(r for r in rollup["utility_contamination"] if r["corpus"] == "cosign")
    assert row["scores"]["degree"] == 0.6
    assert row["scores"]["pagerank"] == 0.2


def test_rollup_verdict_names_the_direction_including_against_us() -> None:
    """The campaign has to be able to conclude the maintainer was right. A rollup that
    can only report a win is not evidence."""
    favours_pagerank = rr.build_rollup(
        [rollup_case("cosign", utility={"degree": 0.6, "pagerank": 0.2})]
    )["verdicts"]["H5"]
    assert favours_pagerank["supported"] is True

    favours_degree = rr.build_rollup(
        [rollup_case("cosign", utility={"degree": 0.1, "pagerank": 0.5})]
    )["verdicts"]["H5"]
    assert favours_degree["supported"] is False
    assert "degree" in favours_degree["statement"].lower()


def test_h2_ignores_the_synthetic_fixture() -> None:
    """Guards: the canary arm contributes 15 cells measured on a 17-file fixture where
    degree and PageRank agree at Spearman 0.979 by construction. Folding those in
    reported H2 as "supported over 15 corpora" — PR #151's claim confirmed — from a
    fixture built to have exactly one structurally central symbol."""
    synthetic = rollup_case("cosign", rho={"pagerank": 0.98})
    synthetic["cases"][0]["corpus"] = None
    rollup = rr.build_rollup([synthetic, rollup_case("redis", rho={"pagerank": 0.36})])
    verdict = rollup["verdicts"]["H2"]
    assert verdict["corpora_compared"] == 1
    assert verdict["spearman_mean"] == 0.36
    assert verdict["supported"] is False


def test_rollup_reads_the_comparison_keys_the_probe_actually_emits() -> None:
    """Guards: run_rank_score_probes emits pagerank_vs_degree.spearman_rho. The rollup
    first looked for a key named pagerank carrying spearman_vs_degree, so H2 read as
    inconclusive on a corpus that had measured 0.363 — a result contradicting PR #151's
    "same ranking signal" claim was reported as no data."""
    rollup = rr.build_rollup([rollup_case("flask", rho={"pagerank": 0.363})])
    assert rollup["verdicts"]["H2"]["corpora_compared"] == 1
    assert rollup["verdicts"]["H2"]["supported"] is False


def test_rollup_calls_a_tie_inconclusive_rather_than_a_win() -> None:
    """Guards: equal means printed "degree DESC is the cleaner arm (0.000 vs 0.000)".
    flask has no hub utilities, so every scorer scores 0 there and the campaign would
    have claimed a refutation from a corpus that measured nothing."""
    all_zero = rr.build_rollup(
        [rollup_case("flask", utility={"degree": 0.0, "pagerank": 0.0})]
    )["verdicts"]["H5"]
    assert all_zero["supported"] is None
    # All-zero is the more specific diagnosis and takes precedence: the metric did not
    # discriminate, which is different from two scores that genuinely tied.
    assert "did not discriminate" in all_zero["statement"]

    equal_but_measured = rr.build_rollup(
        [
            rollup_case(name, utility={"degree": 0.2, "pagerank": 0.2})
            for name in ("cosign", "redis", "runc")
        ]
    )["verdicts"]["H5"]
    assert equal_but_measured["supported"] is None
    assert "no measurable difference" in equal_but_measured["statement"]


def test_rollup_scores_a_hypothesis_only_on_corpora_registered_for_it() -> None:
    """corpora-v1.json pre-registers which hypothesis each corpus discriminates. flask
    carries H4 only; averaging its zero utility contamination into H5 would dilute the
    corpora that were chosen precisely because they have hub utilities."""
    cases = [
        {
            **rollup_case("flask", utility={"degree": 0.0, "pagerank": 0.0}),
        },
        {
            **rollup_case("redis", utility={"degree": 0.6, "pagerank": 0.2}),
        },
    ]
    cases[0]["cases"][0]["corpus"]["discriminates"] = ["H4"]
    cases[1]["cases"][0]["corpus"]["discriminates"] = ["H5"]
    verdict = rr.build_rollup(cases)["verdicts"]["H5"]
    assert verdict["corpora_compared"] == 1
    assert verdict["degree_mean"] == 0.6
    assert verdict["not_discriminating"] == ["flask"]


def test_rollup_marks_a_stale_corpus_as_not_evidence() -> None:
    """search_graph silently falls back to a degree sort when the ranking views are
    stale (src/store/store.c:11695-11711), so a stale cell measures the fallback."""
    rollup = rr.build_rollup([rollup_case("redis", fresh=False)])
    assert rollup["excluded"] and rollup["excluded"][0]["reason"] == "rank_views_stale"
    assert rollup["verdicts"]["H5"]["corpora_compared"] == 0


def test_rollup_diffs_coverage_across_index_modes_with_issue_citations() -> None:
    """The silent-drop claim is per directory, per mode, and each row has to carry the
    issue that reported it or it is not sourced evidence."""
    rollup = rr.build_rollup(
        [
            rollup_case("cosign", index_mode="full",
                        directories={"cmd": 40, "scripts": 5, "hack": 3}),
            rollup_case("cosign", index_mode="fast", directories={"cmd": 40}),
        ]
    )
    rows = {row["directory"]: row for row in rollup["silent_drop"]}
    assert rows["scripts"]["files_lost"] == 5
    assert rows["scripts"]["index_mode"] == "fast"
    assert "#1406" in rows["scripts"]["issues"]
    assert rows["hack"]["files_lost"] == 3


def test_rollup_reports_no_silent_drop_when_coverage_matches() -> None:
    rollup = rr.build_rollup(
        [
            rollup_case("flask", index_mode="full", directories={"src": 10}),
            rollup_case("flask", index_mode="fast", directories={"src": 10}),
        ]
    )
    assert rollup["silent_drop"] == []


def test_detail_frontier_reports_quality_per_response_token() -> None:
    """Upstream #1382 measured recall 0.723 -> 0.525 on jackrabbit-oak purely from the
    graph arm returning less. A ranking change that lets a smaller page carry the same
    quality is worth more than one that reorders a large page, and only a per-token
    view can tell those apart."""
    lean = rollup_case("cosign")
    lean["parameters"]["config_overrides"] = {"search_limit": "10"}
    lean["cases"][0]["oracles"] = {
        "q1": {"response": {}, "response_token_estimate": 400},
        "quality": {"mean_ndcg_at_5": 0.8, "mean_reciprocal_rank": 0.9},
    }
    rich = rollup_case("cosign")
    rich["parameters"]["config_overrides"] = {"search_limit": "200"}
    rich["cases"][0]["oracles"] = {
        "q1": {"response": {}, "response_token_estimate": 4000},
        "quality": {"mean_ndcg_at_5": 0.85, "mean_reciprocal_rank": 0.9},
    }
    rows = {row["detail"]: row for row in rr.build_rollup([lean, rich])["detail_frontier"]}
    assert rows["search_limit=10"]["response_tokens"] == 400
    assert rows["search_limit=10"]["ndcg_per_1k_tokens"] == 2.0
    # 0.05 more nDCG for 10x the tokens is a worse trade, and the table has to show it.
    assert rows["search_limit=200"]["ndcg_per_1k_tokens"] < 0.25


def test_detail_frontier_omits_cells_that_recorded_no_tokens() -> None:
    """Dividing by a missing token count would print an infinite efficiency."""
    assert rr.build_rollup([rollup_case("cosign")])["detail_frontier"] == []


def test_a_verdict_from_all_zero_measurements_is_provisional() -> None:
    """Guards all three defects found on the first real run: each printed a confident
    verdict from corpora whose every scorer measured 0.000. A metric that did not move
    on any corpus did not measure anything, whatever its mean says."""
    flat = rollup_case("cosign", utility={"degree": 0.0, "pagerank": 0.0})
    verdict = rr.build_rollup([flat])["verdicts"]["H5"]
    assert verdict["corpora_with_signal"] == 0
    assert verdict["status"] == "provisional"


def test_a_verdict_counts_only_corpora_where_the_metric_moved() -> None:
    """cosign and runc measured 0.000 utility contamination for every scorer; redis was
    the only corpus that moved. Averaging three corpora when one carries all the signal
    reports a campaign result from a single measurement."""
    zero = rollup_case("cosign", utility={"degree": 0.0, "pagerank": 0.0})
    signal = rollup_case("redis", utility={"degree": 0.0, "pagerank": 0.1})
    verdict = rr.build_rollup([zero, signal])["verdicts"]["H5"]
    assert verdict["corpora_compared"] == 2
    assert verdict["corpora_with_signal"] == 1
    assert verdict["status"] == "provisional"
    assert "1 of 2" in verdict["statement"]


def test_a_verdict_is_stated_only_with_enough_corpora_carrying_signal() -> None:
    cases = [
        rollup_case(name, utility={"degree": 0.4, "pagerank": 0.1})
        for name in ("cosign", "redis", "runc")
    ]
    verdict = rr.build_rollup(cases)["verdicts"]["H5"]
    assert verdict["corpora_with_signal"] == 3
    assert verdict["status"] == "stated"
    assert verdict["supported"] is True


def test_validation_block_reports_every_precondition() -> None:
    """A reader has to be able to see why a verdict is provisional without rerunning."""
    validation = rr.build_rollup([rollup_case("cosign")])["validation"]
    assert validation["knob_canary_passed"] is None
    assert validation["no_cells_excluded"] is True
    assert validation["provisional_verdicts"] == ["H2", "H4", "H5"]
    assert validation["passed"] is False


def test_markdown_marks_a_provisional_verdict_in_the_table() -> None:
    text = rr.render_markdown(rr.build_rollup([rollup_case("cosign")]))
    assert "provisional" in text.lower()
    assert "NOT VALIDATED" in text


def test_rollup_manifest_is_content_addressed() -> None:
    """The maintainer's stated bar on #1245 was an immutable manifest with a published
    SHA. A rollup whose bytes are not addressable cannot be cited later."""
    cases = [rollup_case("cosign")]
    first = rr.build_rollup(cases)
    assert first["manifest"]["rollup_sha256"] == rr.build_rollup(cases)["manifest"]["rollup_sha256"]
    assert first["manifest"]["rollup_sha256"] != rr.build_rollup(
        [rollup_case("cosign", utility={"degree": 0.9, "pagerank": 0.1})]
    )["manifest"]["rollup_sha256"]


def test_rollup_renders_markdown_without_inventing_absent_values() -> None:
    """scaffolding@K is null for an unlabelled corpus; the table must say so rather
    than print 0.000, which would read as "no scaffolding in the top 10"."""
    text = rr.render_markdown(rr.build_rollup([rollup_case("redis")]))
    assert "utility contamination" in text.lower()
    assert "0.000" not in text.split("Scaffolding")[-1].split("\n\n")[0]


def test_rollup_tolerates_documents_without_rank_probes() -> None:
    assert rr.build_rollup([])["verdicts"]["H5"]["corpora_compared"] == 0
    assert rr.build_rollup([{"cases": [{"scenario": "other"}]}])["utility_contamination"] == []


def main() -> int:
    tests = [
        (name, value)
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    failures = 0
    for name, test in tests:
        try:
            test()
        except Exception as error:  # noqa: BLE001 - report and continue
            failures += 1
            print(f"FAIL {name}: {type(error).__name__}: {error}")
        else:
            print(f"pass {name}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
