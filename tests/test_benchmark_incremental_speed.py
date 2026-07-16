import importlib.util
import gzip
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark-incremental-speed.py"
SPEC = importlib.util.spec_from_file_location("benchmark_incremental_speed", SCRIPT)
assert SPEC and SPEC.loader
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


class BenchmarkIncrementalSpeedTest(unittest.TestCase):
    def test_declared_stale_views_are_collected_from_tool_responses(self) -> None:
        oracles = {
            "search": {
                "freshness": {
                    "state": "stale_with_warning",
                    "stale_views": ["semantic_edges", "pagerank"],
                }
            },
            "architecture": {
                "freshness": {
                    "state": "stale_with_warning",
                    "stale_views": ["architecture", "pagerank"],
                }
            },
            "quality": {"passed": True},
        }

        self.assertEqual(
            BENCHMARK.declared_stale_views(oracles),
            ["architecture", "pagerank", "semantic_edges"],
        )

    def test_declared_stale_semantic_edges_preserve_core_graph_gate(self) -> None:
        gate = BENCHMARK.graph_gate_for_publish_kind(
            {"equal": False},
            BENCHMARK.PUBLISH_INCREMENTAL_EXACT,
            freshness_scoped={
                "equal": True,
                "declared_stale_views": ["semantic_edges"],
                "excluded_edge_types": ["SEMANTICALLY_RELATED"],
            },
        )

        self.assertTrue(gate["passed"])
        self.assertEqual(gate["policy"], "declared_stale_derived_views")
        self.assertFalse(gate["canonical_equal"])
        self.assertTrue(gate["freshness_scoped_equal"])
        self.assertEqual(gate["declared_stale_views"], ["semantic_edges"])

    def test_declared_stale_semantic_edges_do_not_hide_core_graph_mismatch(self) -> None:
        gate = BENCHMARK.graph_gate_for_publish_kind(
            {"equal": False},
            BENCHMARK.PUBLISH_INCREMENTAL_EXACT,
            freshness_scoped={
                "equal": False,
                "kind": "canonical edges excluding declared stale views",
                "declared_stale_views": ["semantic_edges"],
                "excluded_edge_types": ["SEMANTICALLY_RELATED"],
            },
        )

        self.assertFalse(gate["passed"])
        self.assertEqual(gate["policy"], "canonical_graph")

    def test_freshness_scoped_comparison_excludes_only_semantic_edges(self) -> None:
        def create_graph(database: Path, extra_type: str) -> None:
            with sqlite3.connect(database) as con:
                con.execute(
                    "CREATE TABLE nodes("
                    "id INTEGER PRIMARY KEY, project TEXT, label TEXT, name TEXT, "
                    "qualified_name TEXT, file_path TEXT, start_line INTEGER, end_line INTEGER, "
                    "properties TEXT)"
                )
                con.execute(
                    "CREATE TABLE edges("
                    "project TEXT, source_id INTEGER, target_id INTEGER, type TEXT, properties TEXT)"
                )
                con.execute(
                    "CREATE TABLE file_hashes("
                    "project TEXT, rel_path TEXT, sha256 TEXT, mtime_ns INTEGER, size INTEGER)"
                )
                con.executemany(
                    "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?)",
                    [
                        (1, "repo", "Function", "left", "repo.left", "a.c", 1, 2, "{}"),
                        (2, "repo", "Function", "right", "repo.right", "a.c", 4, 5, "{}"),
                    ],
                )
                con.execute(
                    "INSERT INTO edges VALUES ('repo',1,2,?,?)",
                    (extra_type, '{"score":0.75}' if extra_type == "SEMANTICALLY_RELATED" else "{}"),
                )
                con.execute(
                    "INSERT INTO file_hashes VALUES ('repo','a.c','abc',1,10)"
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            empty = root / "empty.db"
            semantic = root / "semantic.db"
            core = root / "core.db"
            create_graph(empty, "SEMANTICALLY_RELATED")
            with sqlite3.connect(empty) as con:
                con.execute("DELETE FROM edges")
            create_graph(semantic, "SEMANTICALLY_RELATED")
            create_graph(core, "CALLS")

            semantic_result = BENCHMARK.compare_graph_excluding_declared_stale_views(
                semantic, empty, "repo", ["semantic_edges"]
            )
            core_result = BENCHMARK.compare_graph_excluding_declared_stale_views(
                core, empty, "repo", ["semantic_edges"]
            )

        self.assertIsNotNone(semantic_result)
        self.assertTrue(semantic_result["equal"])
        self.assertIsNotNone(core_result)
        self.assertFalse(core_result["equal"])
        self.assertEqual(
            core_result["kind"], "canonical edges excluding declared stale views"
        )

    def test_cli_default_preserves_candidate_rank_refresh_policy(self) -> None:
        with mock.patch.object(sys, "argv", [str(SCRIPT)]):
            args = BENCHMARK.parse_args()

        self.assertEqual(args.rank_refresh, BENCHMARK.RANK_REFRESH_CANDIDATE_DEFAULT)

    def test_candidate_default_rank_refresh_does_not_write_config_override(self) -> None:
        with mock.patch.object(BENCHMARK, "run_config_set") as run:
            applied = BENCHMARK.apply_rank_refresh_override(
                Path("/tmp/cbm"), {}, BENCHMARK.RANK_REFRESH_CANDIDATE_DEFAULT, 30
            )

        self.assertFalse(applied)
        run.assert_not_called()

    def test_explicit_rank_refresh_writes_config_override(self) -> None:
        with mock.patch.object(BENCHMARK, "run_config_set") as run:
            applied = BENCHMARK.apply_rank_refresh_override(
                Path("/tmp/cbm"), {}, "stale_on_exact", 30
            )

        self.assertTrue(applied)
        run.assert_called_once_with(
            Path("/tmp/cbm"), {}, "rank_refresh", "stale_on_exact", 30
        )

    def test_stream_query_fingerprint_is_ordered_bounded_and_change_sensitive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "graph.db"
            with sqlite3.connect(database) as con:
                con.execute("CREATE TABLE rows(value TEXT NOT NULL)")
                con.executemany("INSERT INTO rows VALUES (?)", [("beta",), ("alpha",)])

            first = BENCHMARK.stream_query_fingerprint(
                database, "SELECT value FROM rows ORDER BY value", ()
            )
            second = BENCHMARK.stream_query_fingerprint(
                database, "SELECT value FROM rows ORDER BY value", ()
            )
            with sqlite3.connect(database) as con:
                con.execute("INSERT INTO rows VALUES ('gamma')")
            changed = BENCHMARK.stream_query_fingerprint(
                database, "SELECT value FROM rows ORDER BY value", ()
            )

        self.assertEqual(first, second)
        self.assertEqual(first["row_count"], 2)
        self.assertEqual(len(first["sha256"]), 64)
        self.assertEqual(changed["row_count"], 3)
        self.assertNotEqual(changed["sha256"], first["sha256"])

    def test_content_fingerprint_excludes_volatile_file_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fingerprints = []
            canonical_hashes = []
            for index, mtime_ns in enumerate((100, 900)):
                database = Path(tmpdir) / f"graph-{index}.db"
                with sqlite3.connect(database) as con:
                    con.execute(
                        "CREATE TABLE file_hashes("
                        "project TEXT, rel_path TEXT, sha256 TEXT, mtime_ns INTEGER, size INTEGER)"
                    )
                    con.execute(
                        "INSERT INTO file_hashes VALUES ('repo','src/a.c','abc',?,12)",
                        (mtime_ns,),
                    )
                fingerprints.append(
                    BENCHMARK.stream_query_fingerprint(
                        database, BENCHMARK.CONTENT_HASHES_SQL, ("repo",)
                    )
                )
                canonical_hashes.append(
                    BENCHMARK.stream_query_fingerprint(
                        database, BENCHMARK.CANONICAL_HASHES_SQL, ("repo",)
                    )
                )

        self.assertEqual(fingerprints[0], fingerprints[1])
        self.assertNotEqual(canonical_hashes[0], canonical_hashes[1])

    def test_graph_fingerprint_normalizes_project_root_but_retains_semantic_score(self) -> None:
        def create_graph(database: Path, project: str, score: float) -> None:
            with sqlite3.connect(database) as con:
                con.execute(
                    "CREATE TABLE nodes("
                    "id INTEGER PRIMARY KEY, project TEXT, label TEXT, name TEXT, "
                    "qualified_name TEXT, file_path TEXT, start_line INTEGER, end_line INTEGER, "
                    "properties TEXT)"
                )
                con.execute(
                    "CREATE TABLE edges("
                    "project TEXT, source_id INTEGER, target_id INTEGER, type TEXT, properties TEXT)"
                )
                con.execute(
                    "CREATE TABLE file_hashes("
                    "project TEXT, rel_path TEXT, sha256 TEXT, mtime_ns INTEGER, size INTEGER)"
                )
                con.executemany(
                    "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?)",
                    [
                        (1, project, "Function", "left", f"{project}.pkg.left", "src/a.py", 1, 2,
                         json.dumps({"checkout": f"/tmp/{project}"})),
                        (2, project, "Function", "right", f"{project}.pkg.right", "src/a.py", 4, 5,
                         json.dumps({"checkout": f"/tmp/{project}"})),
                        (3, project, "Project", project, project, "", 0, 0,
                         json.dumps({"root": f"/tmp/{project}"})),
                    ],
                )
                con.execute(
                    "INSERT INTO edges VALUES (?,?,?,?,?)",
                    (project, 1, 2, "SEMANTICALLY_RELATED", json.dumps({"score": score})),
                )
                con.execute(
                    "INSERT INTO file_hashes VALUES (?,?,?,?,?)",
                    (project, "src/a.py", "content-sha", 123, 42),
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            left_db = Path(tmpdir) / "left.db"
            right_db = Path(tmpdir) / "right.db"
            create_graph(left_db, "random-root-a", 0.873)
            create_graph(right_db, "random-root-b", 0.873)

            left = BENCHMARK.stable_graph_fingerprint(left_db, "random-root-a")
            right = BENCHMARK.stable_graph_fingerprint(right_db, "random-root-b")
            self.assertEqual(left, right)

            with sqlite3.connect(right_db) as con:
                con.execute(
                    "UPDATE edges SET properties = ?",
                    (json.dumps({"score": 0.811}),),
                )
            changed = BENCHMARK.stable_graph_fingerprint(right_db, "random-root-b")

        self.assertEqual(left["components"]["nodes"], changed["components"]["nodes"])
        self.assertEqual(left["components"]["edges"], changed["components"]["edges"])
        self.assertNotEqual(
            left["components"]["semantic_scores"],
            changed["components"]["semantic_scores"],
        )

    def test_archive_measurement_log_streams_reproducible_gzip_with_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "worker.log"
            artifacts = root / "artifacts"
            payload = ("level=info msg=mem.phase peak_mb=64\n" * 100).encode()
            source.write_bytes(payload)

            first = BENCHMARK.archive_measurement_log(source, artifacts)
            second = BENCHMARK.archive_measurement_log(source, artifacts)
            archive = artifacts / first["artifact_name"]

            self.assertEqual(first, second)
            self.assertEqual(gzip.decompress(archive.read_bytes()), payload)
            self.assertEqual(first["source_bytes"], len(payload))
            self.assertEqual(len(first["source_sha256"]), 64)
            self.assertEqual(len(first["artifact_sha256"]), 64)
            self.assertEqual(len(list(artifacts.glob("*.log.gz"))), 1)

    def test_copy_git_revision_to_dir_excludes_dirty_and_untracked_source_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.email", "benchmark@example.invalid"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.name", "Benchmark Fixture"], cwd=source, check=True)
            (source / "tracked.py").write_text("VERSION = 1\n", encoding="utf-8")
            task_source = source / "benchmarks" / "semantic-pairs-v1" / "canary.py"
            task_source.parent.mkdir(parents=True)
            task_source.write_text("DUPLICATE = True\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked.py", str(task_source.relative_to(source))], cwd=source, check=True)
            subprocess.run(["git", "commit", "-q", "-m", "fixture"], cwd=source, check=True)
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=source, check=True, text=True, capture_output=True
            ).stdout.strip()
            (source / "tracked.py").write_text("VERSION = 2\n", encoding="utf-8")
            (source / "untracked.py").write_text("UNTRACKED = True\n", encoding="utf-8")

            metadata = BENCHMARK.copy_git_revision_to_dir(
                source,
                destination,
                revision,
                timeout=30,
                excluded_prefixes=("benchmarks/semantic-pairs-v1/",),
            )

            self.assertEqual((destination / "tracked.py").read_text(), "VERSION = 1\n")
            self.assertFalse((destination / "untracked.py").exists())
            self.assertFalse((destination / "benchmarks" / "semantic-pairs-v1").exists())
            self.assertEqual(metadata["revision"], revision)
            self.assertRegex(metadata["tree"], r"^[0-9a-f]{40}$")
            self.assertIn("tracked.py", metadata["source_dirty_status_short"])
            self.assertFalse((destination / ".git").exists())
            self.assertEqual(metadata["excluded_prefixes"], ["benchmarks/semantic-pairs-v1/"])

    def test_pair_classification_scores_explicit_positive_and_negative_judgments(self) -> None:
        judgments = [
            {
                "source": "fixture.alpha",
                "target": "fixture.beta",
                "expected": True,
                "category": "near_clone",
            },
            {
                "source": "fixture.alpha",
                "target": "fixture.decoy",
                "expected": False,
                "category": "lexical_hard_negative",
            },
            {
                "source": "fixture.gamma",
                "target": "fixture.delta",
                "expected": True,
                "category": "near_clone",
            },
            {
                "source": "fixture.gamma",
                "target": "fixture.decoy",
                "expected": False,
                "category": "unrelated_negative",
            },
        ]
        observed = [
            {"source": "fixture.beta", "target": "fixture.alpha", "score": 0.98},
            {"source": "fixture.alpha", "target": "fixture.decoy", "score": 0.96},
            {"source": "background.one", "target": "background.two", "score": 0.97},
        ]

        result = BENCHMARK.score_pair_classification(observed, judgments)

        self.assertEqual(result["confusion"], {"tp": 1, "fp": 1, "fn": 1, "tn": 1})
        self.assertEqual(result["precision"], 0.5)
        self.assertEqual(result["recall"], 0.5)
        self.assertEqual(result["f1"], 0.5)
        self.assertEqual(result["false_positive_rate"], 0.5)
        self.assertEqual(result["unjudged_observed_count"], 1)
        self.assertEqual(result["unjudged_observed"][0]["source"], "background.one")
        self.assertEqual(result["categories"]["near_clone"]["tp"], 1)
        self.assertEqual(result["categories"]["near_clone"]["fn"], 1)
        self.assertEqual(result["categories"]["lexical_hard_negative"]["fp"], 1)

    def test_pair_classification_rejects_duplicate_or_conflicting_judgments(self) -> None:
        duplicate = [
            {"source": "fixture.a", "target": "fixture.b", "expected": True},
            {"source": "fixture.b", "target": "fixture.a", "expected": True},
        ]
        conflicting = [
            {"source": "fixture.a", "target": "fixture.b", "expected": True},
            {"source": "fixture.b", "target": "fixture.a", "expected": False},
        ]

        with self.assertRaisesRegex(ValueError, "duplicate pair judgment"):
            BENCHMARK.score_pair_classification([], duplicate)
        with self.assertRaisesRegex(ValueError, "duplicate pair judgment"):
            BENCHMARK.score_pair_classification([], conflicting)

    def test_pair_classification_reports_undefined_denominators_as_null(self) -> None:
        result = BENCHMARK.score_pair_classification(
            [],
            [
                {
                    "source": "fixture.a",
                    "target": "fixture.b",
                    "expected": False,
                    "category": "negative",
                }
            ],
        )

        self.assertIsNone(result["precision"])
        self.assertIsNone(result["recall"])
        self.assertIsNone(result["f1"])
        self.assertEqual(result["false_positive_rate"], 0.0)

    def test_similarity_quality_fixture_has_versioned_pair_judgments_and_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = BENCHMARK.create_similarity_quality_repo(Path(tmpdir))
            source = (Path(tmpdir) / "cbmq_similarity.go").read_text()

        self.assertEqual(fixture["capability"], "similarity")
        self.assertEqual(fixture["relationship"], "SIMILAR_TO")
        self.assertEqual(fixture["task_set_version"], "semantic-pairs-v1")
        self.assertRegex(fixture["task_set_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(len(fixture["source_sha256"]), 1)
        self.assertTrue(any(item["expected"] for item in fixture["judgments"]))
        self.assertTrue(any(not item["expected"] for item in fixture["judgments"]))
        self.assertIn("cbmqValidateUser", source)
        self.assertIn("cbmqValidateOrder", source)
        self.assertIn("cbmqValidateProfileDecoy", source)

    def test_semantic_edges_quality_fixture_is_distinct_from_similarity_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = BENCHMARK.create_semantic_edges_quality_repo(Path(tmpdir))
            source = (Path(tmpdir) / "cbmq_records.py").read_text()

        self.assertEqual(fixture["capability"], "semantic_edges")
        self.assertEqual(fixture["relationship"], "SEMANTICALLY_RELATED")
        self.assertEqual(fixture["task_set_version"], "semantic-pairs-v1")
        self.assertTrue(any(item["expected"] for item in fixture["judgments"]))
        self.assertTrue(any(not item["expected"] for item in fixture["judgments"]))
        self.assertIn("cbmq_normalize_user_record", source)
        self.assertIn("cbmq_normalize_account_record", source)
        self.assertIn("cbmq_archive_record_decoy", source)

    def test_pair_quality_mutation_replaces_exact_source_and_changes_judgments(self) -> None:
        for factory, expected_added, expected_removed in (
            (
                BENCHMARK.create_similarity_quality_repo,
                ("cbmqValidateProfileDecoy", "cbmqValidateUser"),
                ("cbmqValidateOrder", "cbmqValidateUser"),
            ),
            (
                BENCHMARK.create_semantic_edges_quality_repo,
                ("cbmq_archive_record_decoy", "cbmq_normalize_user_record"),
                ("cbmq_normalize_account_record", "cbmq_normalize_user_record"),
            ),
        ):
            with self.subTest(factory=factory.__name__), tempfile.TemporaryDirectory() as tmpdir:
                repo = Path(tmpdir)
                fixture = factory(repo)
                mutation = BENCHMARK.apply_pair_quality_mutation(repo, fixture)

                self.assertEqual(mutation["changed_paths"], [fixture["source_paths"][0]])
                self.assertNotEqual(mutation["before_sha256"], mutation["after_sha256"])
                post_expected = {
                    BENCHMARK.canonical_pair(item["source"], item["target"]): item["expected"]
                    for item in mutation["post_judgments"]
                }
                self.assertTrue(post_expected[BENCHMARK.canonical_pair(*expected_added)])
                self.assertFalse(post_expected[BENCHMARK.canonical_pair(*expected_removed)])

    def test_relation_quality_oracle_scores_raw_query_rows_and_response_cost(self) -> None:
        calls = []
        original = BENCHMARK.run_tool_call_for_transport

        def fake_call(*args, **kwargs):
            calls.append((args[3], args[4]))
            return {
                "elapsed_ms": 4.25,
                "response_bytes": 211,
                "response_token_estimate": 53,
                "response": {
                    "columns": ["a.name", "b.name", "r.jaccard", "a.file_path", "b.file_path"],
                    "rows": [
                        [
                            "cbmqValidateOrder",
                            "cbmqValidateUser",
                            "0.984",
                            "cbmq_similarity.go",
                            "cbmq_similarity.go",
                        ]
                    ],
                },
            }

        class Args:
            timeout = 10
            include_logs = False

        fixture = {
            "relationship": "SIMILAR_TO",
            "score_property": "jaccard",
            "query_name_marker": "cbmq",
            "judgments": [
                {
                    "source": "cbmqValidateUser",
                    "target": "cbmqValidateOrder",
                    "expected": True,
                    "category": "structural_near_clone",
                },
                {
                    "source": "cbmqValidateUser",
                    "target": "cbmqValidateProfileDecoy",
                    "expected": False,
                    "category": "lexical_hard_negative",
                },
            ],
        }
        BENCHMARK.run_tool_call_for_transport = fake_call
        try:
            result = BENCHMARK.run_relation_quality_oracles(
                "cli", Path("cbm"), {}, "fixture", fixture, Args()
            )
        finally:
            BENCHMARK.run_tool_call_for_transport = original

        self.assertEqual(calls[0][0], "query_graph")
        self.assertIn("SIMILAR_TO", calls[0][1]["query"])
        self.assertEqual(calls[0][1]["format"], "json")
        self.assertEqual(result["pair_classification"]["confusion"], {
            "tp": 1,
            "fp": 0,
            "fn": 0,
            "tn": 1,
        })
        self.assertTrue(result["passed"])
        self.assertEqual(result["response_quality"]["response_bytes"], 211)
        self.assertEqual(result["observed_pairs"][0]["score"], 0.984)

    def test_pair_oracle_equality_is_order_independent_but_score_sensitive(self) -> None:
        incremental = {
            "observed_pairs": [
                {"source": "b", "target": "a", "score": 0.87},
                {"source": "c", "target": "a", "score": 0.91},
            ]
        }
        fresh = {
            "observed_pairs": [
                {"source": "a", "target": "c", "score": 0.91},
                {"source": "a", "target": "b", "score": 0.87},
            ]
        }

        equal = BENCHMARK.compare_pair_oracle_outputs(incremental, fresh)
        self.assertTrue(equal["passed"])

        fresh["observed_pairs"][0]["score"] = 0.90
        unequal = BENCHMARK.compare_pair_oracle_outputs(incremental, fresh)
        self.assertFalse(unequal["passed"])
        self.assertEqual(len(unequal["incremental_only"]), 1)
        self.assertEqual(len(unequal["fresh_only"]), 1)

    def test_pair_incremental_policy_distinguishes_default_stale_from_eager_freshness(self) -> None:
        stale_index = {"publish_kind": "incremental_exact"}
        stale_oracles = {
            "passed": False,
            "edge_query": {
                "response": {
                    "warnings": [
                        "semantic_edges derived view is stale; query_graph semantic edges may be stale."
                    ]
                }
            },
        }
        stale = BENCHMARK.evaluate_pair_incremental_policy(
            {}, stale_index, stale_oracles, {"equal": False}, {"passed": False}
        )
        self.assertEqual(stale["policy"], "stale_on_incremental")
        self.assertFalse(stale["immediate_freshness_expected"])
        self.assertTrue(stale["policy_conformance_met"])

        eager = BENCHMARK.evaluate_pair_incremental_policy(
            {"incremental_derived_refresh": "eager"},
            stale_index,
            {"passed": True, "edge_query": {"response": {}}},
            {"equal": True},
            {"passed": True},
        )
        self.assertTrue(eager["immediate_freshness_expected"])
        self.assertTrue(eager["immediate_freshness_met"])
        self.assertTrue(eager["policy_conformance_met"])

    def test_search_projection_observation_separates_identity_and_property_fields(self) -> None:
        data = {
            "results": [
                {
                    "qualified_name": "fixture.Func0000_00",
                    "label": "Function",
                    "file_path": "pkg/file_0000.go",
                    "source": "project",
                    "complexity": 1,
                    "fp": "opaque",
                }
            ]
        }

        observation = BENCHMARK.build_search_projection_observation(
            "compact_fields", data, 400, 2.5, True
        )

        self.assertEqual(observation["qualified_names"], ["fixture.Func0000_00"])
        self.assertEqual(observation["property_fields"], ["complexity", "fp"])
        self.assertEqual(observation["internal_fields"], ["fp"])
        self.assertFalse(observation["passed"])
        self.assertEqual(observation["response_bytes"], len(BENCHMARK.canonical_response_bytes(data)))

    def test_parse_list_project_counts_requires_strictly_increasing_positive_values(self) -> None:
        self.assertEqual(BENCHMARK.parse_list_project_counts("1,16,64"), [1, 16, 64])
        for invalid in ("", "0,1", "1,1", "16,1", "1,two"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                BENCHMARK.parse_list_project_counts(invalid)

    def test_clone_list_project_db_rekeys_rows_without_mutating_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            seed = Path(tmpdir) / "seed.db"
            clone = Path(tmpdir) / "clone.db"
            with sqlite3.connect(seed) as con:
                con.executescript(
                    "CREATE TABLE projects(name TEXT PRIMARY KEY, root_path TEXT);"
                    "CREATE TABLE nodes(id INTEGER PRIMARY KEY, project TEXT);"
                    "CREATE TABLE edges(id INTEGER PRIMARY KEY, project TEXT);"
                    "INSERT INTO projects VALUES('seed','/seed');"
                    "INSERT INTO nodes VALUES(1,'seed');"
                    "INSERT INTO edges VALUES(1,'seed');"
                )

            BENCHMARK.clone_list_project_db(seed, clone, "clone", "/clone")

            with sqlite3.connect(seed) as con:
                self.assertEqual(con.execute("SELECT name FROM projects").fetchone()[0], "seed")
            with sqlite3.connect(clone) as con:
                self.assertEqual(
                    con.execute("SELECT name, root_path FROM projects").fetchone(),
                    ("clone", "/clone"),
                )
                self.assertEqual(con.execute("SELECT project FROM nodes").fetchone()[0], "clone")
                self.assertEqual(con.execute("SELECT project FROM edges").fetchone()[0], "clone")

    def test_list_project_fixture_budget_enforces_cap_and_free_space_reserve(self) -> None:
        mib = 1024 * 1024
        budget = BENCHMARK.list_project_fixture_budget(
            seed_bytes=mib,
            maximum_projects=64,
            maximum_fixture_mb=64,
            disk_free_bytes=4 * 1024 * mib,
        )
        self.assertTrue(budget["passed"])
        self.assertEqual(budget["projected_fixture_bytes"], 64 * mib)
        self.assertEqual(budget["reserved_free_bytes"], 2 * 1024 * mib)

        capped = BENCHMARK.list_project_fixture_budget(
            seed_bytes=mib,
            maximum_projects=64,
            maximum_fixture_mb=63,
            disk_free_bytes=4 * 1024 * mib,
        )
        self.assertFalse(capped["passed"])
        self.assertEqual(capped["reason"], "projected fixture exceeds configured cap")

        reserve = BENCHMARK.list_project_fixture_budget(
            seed_bytes=3 * 1024 * mib,
            maximum_projects=1,
            maximum_fixture_mb=4096,
            disk_free_bytes=4 * 1024 * mib,
        )
        self.assertFalse(reserve["passed"])
        self.assertEqual(reserve["reason"], "projected fixture violates free-space reserve")

    def test_mcp_client_exit_reaps_process_streams_and_reader_threads(self) -> None:
        class FakeStream:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        class FakeProcess:
            def __init__(self) -> None:
                self.stdin = FakeStream()
                self.stdout = FakeStream()
                self.stderr = FakeStream()
                self.wait_calls = 0

            def wait(self, timeout: int) -> int:
                self.wait_calls += 1
                return 0

        class FakeThread:
            def __init__(self) -> None:
                self.join_calls = 0

            def join(self, timeout: int) -> None:
                self.join_calls += 1

            def is_alive(self) -> bool:
                return False

        client = BENCHMARK.McpClient(Path("cbm"), {}, 10)
        process = FakeProcess()
        stdout_thread = FakeThread()
        stderr_thread = FakeThread()
        client.proc = process
        client.stdout_thread = stdout_thread
        client.stderr_thread = stderr_thread

        client.__exit__(None, None, None)

        self.assertTrue(process.stdin.closed)
        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)
        self.assertEqual(process.wait_calls, 1)
        self.assertEqual(stdout_thread.join_calls, 1)
        self.assertEqual(stderr_thread.join_calls, 1)
        self.assertIsNone(client.proc)

    def test_rank_quality_fixture_separates_graph_signal_from_lexical_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata = BENCHMARK.create_rank_quality_repo(Path(tmpdir))
            core = (Path(tmpdir) / "order_core.py").read_text()
            stubs = (Path(tmpdir) / "order_stubs.py").read_text()
            callers = sorted(Path(tmpdir).glob("caller_*.py"))
            caller_sources = [path.read_text() for path in callers]

        self.assertEqual(metadata["capability"], "rank")
        self.assertEqual(metadata["relevant_symbol"], "zz_order_core")
        self.assertEqual(len(metadata["lexical_decoys"]), 8)
        self.assertIn("def zz_order_core", core)
        self.assertIn("def aa_order_stub", stubs)
        self.assertEqual(len(callers), 8)
        self.assertTrue(all("zz_order_core" in source for source in caller_sources))

    def test_dependency_quality_fixture_has_local_resolvable_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata = BENCHMARK.create_dependency_quality_repo(Path(tmpdir))
            manifest = json.loads((Path(tmpdir) / "package.json").read_text())
            app_source = (Path(tmpdir) / "src" / "app.js").read_text()
            dep_source = (
                Path(tmpdir) / "node_modules" / "cbmbenchdep" / "index.js"
            ).read_text()

        self.assertEqual(metadata["capability"], "dependencies")
        self.assertEqual(manifest["dependencies"], {"cbmbenchdep": "1.0.0"})
        self.assertIn("canonicalDependencyAPI", app_source)
        self.assertIn("canonicalDependencyAPI", dep_source)
        self.assertEqual(metadata["relevant_symbol"], "canonicalDependencyAPI")

    def test_rank_quality_oracle_uses_central_symbol_as_graded_judgment(self) -> None:
        calls = []
        original = BENCHMARK.run_tool_call_for_transport

        def fake_call(*args, **kwargs):
            calls.append((args[3], args[4]))
            return {
                "response": {
                    "results": [
                        {"name": "aa_order_stub"},
                        {"name": "zz_order_core"},
                    ]
                }
            }

        class Args:
            timeout = 10
            include_logs = False

        BENCHMARK.run_tool_call_for_transport = fake_call
        try:
            result = BENCHMARK.run_rank_quality_oracles(
                "cli", Path("cbm"), {}, "fixture", Args()
            )
        finally:
            BENCHMARK.run_tool_call_for_transport = original

        self.assertEqual(calls[0][0], "search_graph")
        self.assertEqual(calls[0][1]["name_pattern"], "order")
        quality = result["central_order_search"]["quality"]
        self.assertEqual(quality["expected_substring"], "zz_order_core")
        self.assertEqual(quality["rank"], 2)
        self.assertEqual(quality["reciprocal_rank"], 0.5)
        self.assertIsNotNone(quality["ndcg_at_5"])

    def test_dependency_quality_oracle_requires_dependency_provenance(self) -> None:
        calls = []
        original = BENCHMARK.run_tool_call_for_transport

        def fake_call(*args, **kwargs):
            calls.append((args[3], args[4]))
            return {
                "response": {
                    "results": [
                        {
                            "name": "canonicalDependencyAPI",
                            "source": "dependency",
                            "package": "cbmbenchdep",
                            "read_only": True,
                        }
                    ]
                }
            }

        class Args:
            timeout = 10
            include_logs = False

        BENCHMARK.run_tool_call_for_transport = fake_call
        try:
            result = BENCHMARK.run_dependency_quality_oracles(
                "cli", Path("cbm"), {}, "fixture", Args()
            )
        finally:
            BENCHMARK.run_tool_call_for_transport = original

        self.assertEqual(calls[0][0], "search_graph")
        self.assertTrue(calls[0][1]["include_dependencies"])
        self.assertEqual(calls[0][1]["name_pattern"], "canonicalDependencyAPI")
        quality = result["dependency_api_search"]["quality"]
        self.assertTrue(quality["passed"])
        self.assertEqual(quality["rank"], 1)
        self.assertEqual(
            quality["required_substrings"],
            [
                '\"source\":\"dependency\"',
                '\"package\":\"cbmbenchdep\"',
                '\"read_only\":true',
            ],
        )

    def test_reciprocal_rank_uses_full_bounded_result_beyond_ndcg_cutoff(self) -> None:
        ranked = [{"name": f"decoy_{index}"} for index in range(8)]
        ranked.append({"name": "relevant"})

        result = BENCHMARK.score_ranked_relevance(
            ranked,
            [{"expected_substring": "relevant", "relevance": 3}],
            cutoff=5,
        )

        self.assertEqual(result["first_relevant_rank"], 9)
        self.assertAlmostEqual(result["reciprocal_rank"], 1 / 9)
        self.assertFalse(result["hit_at_5"])
        self.assertEqual(result["ndcg_at_5"], 0.0)
        self.assertEqual(len(result["matched_relevance"]), 5)

    def test_frontier_fixture_counts_dependents_and_mutates_one_definition_file(self) -> None:
        cases = {
            "go_inbound_frontier": ("go", "leaf.go", "LeafExtra"),
            "python_inbound_frontier": ("python", "leaf.py", "leaf_extra"),
            "c_header_inbound_frontier": ("c_header", "shared.h", "shared_extra"),
            "cpp_inbound_frontier": ("cpp", "shared.hpp", "shared_extra"),
            "cuda_inbound_frontier": ("cuda", "shared.cuh", "shared_extra"),
            "javascript_inbound_frontier": ("javascript", "leaf.js", "leafExtra"),
            "typescript_inbound_frontier": ("typescript", "leaf.ts", "leafExtra"),
            "tsx_inbound_frontier": ("tsx", "leaf.tsx", "leafExtra"),
            "php_inbound_frontier": ("php", "Leaf.php", "leaf_extra"),
            "csharp_inbound_frontier": ("csharp", "Leaf.cs", "Extra"),
            "java_inbound_frontier": ("java", "Leaf.java", "extra"),
            "kotlin_inbound_frontier": ("kotlin", "Leaf.kt", "leafExtra"),
            "rust_inbound_frontier": ("rust", "leaf.rs", "leaf_extra"),
        }
        for scenario, (language, changed_path, marker) in cases.items():
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmpdir:
                repo = Path(tmpdir)
                metadata = BENCHMARK.create_inbound_frontier_repo(repo, language, 7)
                changed = BENCHMARK.mutate_inbound_frontier_repo(repo, language)

                self.assertEqual(metadata["language"], language)
                self.assertEqual(metadata["requested_inbound_dependents"], 7)
                resolver_language = "c" if language == "c_header" else language
                if resolver_language in BENCHMARK.SCOPED_EXACT_FRONTIER_LANGUAGES:
                    self.assertEqual(metadata["incremental_contract"], "exact_frontier")
                    self.assertEqual(metadata["expected_minimum_affected_files"], 8)
                else:
                    self.assertEqual(metadata["incremental_contract"], "safe_full_rebuild")
                    self.assertEqual(metadata["expected_publish_kind"], "full")
                    self.assertEqual(metadata["expected_reason"], "scoped_lsp_gap")
                self.assertEqual(changed, [changed_path])
                self.assertIn(marker, (repo / changed_path).read_text(encoding="utf-8"))
                for index in range(7):
                    self.assertTrue((repo / metadata["dependent_paths"][index]).is_file())

    def test_frontier_catalog_matches_cross_file_resolver_languages(self) -> None:
        fixture_languages = {
            "c" if language == "c_header" else language
            for language in BENCHMARK.MATRIX_FRONTIER_SCENARIOS.values()
        }
        self.assertEqual(fixture_languages, set(BENCHMARK.CROSS_FILE_RESOLVER_LANGUAGES))

    def test_frontier_fixture_rejects_nonpositive_dependent_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "frontier files must be positive"):
                BENCHMARK.create_inbound_frontier_repo(Path(tmpdir), "go", 0)

    def test_frontier_gate_rejects_fixture_that_did_not_expand(self) -> None:
        metadata = {"expected_minimum_affected_files": 8}
        incremental = {"response": {"exact_delta": {"affected_paths": 1}}}

        gate = BENCHMARK.frontier_coverage_gate(metadata, incremental)

        self.assertFalse(gate["passed"])
        self.assertEqual(gate["expected_minimum_affected_files"], 8)
        self.assertEqual(gate["observed_affected_files"], 1)
        self.assertEqual(gate["reason"], "observed frontier is smaller than the fixture contract")

    def test_frontier_gate_is_not_applicable_to_nonfrontier_scenarios(self) -> None:
        gate = BENCHMARK.frontier_coverage_gate({}, {"response": {}})

        self.assertTrue(gate["passed"])
        self.assertFalse(gate["applicable"])

    def test_frontier_gate_accepts_declared_scoped_lsp_full_rebuild(self) -> None:
        metadata = {
            "expected_publish_kind": "full",
            "expected_reason": "scoped_lsp_gap",
        }
        incremental = {"publish_kind": "full", "exact_reason": "scoped_lsp_gap"}

        gate = BENCHMARK.frontier_coverage_gate(metadata, incremental)

        self.assertTrue(gate["passed"])
        self.assertEqual(gate["contract"], "safe_full_rebuild")

    def test_frontier_gate_accepts_explicit_configured_cap_fallback(self) -> None:
        metadata = {"expected_minimum_affected_files": 17}
        incremental = {
            "publish_kind": "incremental_containment",
            "exact_reason": "frontier_too_large",
            "response": {
                "exact_delta": {
                    "affected_paths": 16,
                    "affected_paths_limit": 16,
                    "affected_paths_truncated": True,
                }
            },
        }

        gate = BENCHMARK.frontier_coverage_gate(metadata, incremental, exact_cap=16)

        self.assertTrue(gate["passed"])
        self.assertEqual(gate["contract"], "configured_cap_fallback")
        self.assertEqual(gate["expected_minimum_affected_files"], 17)

    def test_frontier_gate_rejects_cap_fallback_without_truncation_evidence(self) -> None:
        metadata = {"expected_minimum_affected_files": 17}
        incremental = {
            "publish_kind": "full",
            "exact_reason": "frontier_too_large",
            "response": {"exact_delta": {"affected_paths_truncated": False}},
        }

        gate = BENCHMARK.frontier_coverage_gate(metadata, incremental, exact_cap=16)

        self.assertFalse(gate["passed"])
        self.assertIn("truncation", gate["reason"])

    def test_minimal_indexing_profile_disables_every_optional_cost_center(self) -> None:
        overrides = BENCHMARK.resolve_config_overrides("minimal_indexing", [])
        self.assertEqual(
            overrides,
            {
                "auto_index_deps": "false",
                "githistory_enabled": "false",
                "httplinks_enabled": "false",
                "rank_enabled": "false",
                "semantic_edges_enabled": "false",
                "similarity_enabled": "false",
            },
        )

    def test_dependency_disabled_profile_changes_only_dependency_indexing(self) -> None:
        self.assertEqual(
            BENCHMARK.resolve_config_overrides("dependency_disabled", []),
            {"auto_index_deps": "false"},
        )

    def test_incremental_semantic_freshness_eager_profile_changes_only_refresh_policy(self) -> None:
        self.assertEqual(
            BENCHMARK.resolve_config_overrides("incremental_semantic_freshness_eager", []),
            {"incremental_derived_refresh": "eager"},
        )

    def test_single_capability_ablation_profiles_change_exactly_one_group(self) -> None:
        expected = {
            "rank_disabled": {"rank_enabled": "false"},
            "similarity_disabled": {"similarity_enabled": "false"},
            "semantic_edges_disabled": {"semantic_edges_enabled": "false"},
            "git_history_disabled": {"githistory_enabled": "false"},
            "http_links_disabled": {"httplinks_enabled": "false"},
            "dependency_disabled": {"auto_index_deps": "false"},
        }

        self.assertEqual(
            {
                profile: BENCHMARK.resolve_config_overrides(profile, [])
                for profile in expected
            },
            expected,
        )

    def test_index_mode_metadata_marks_fast_only_capability_gaps(self) -> None:
        self.assertEqual(
            BENCHMARK.index_mode_capability_applicability("fast"),
            {
                "rank": {"applicable": True, "reason": "available in fast mode"},
                "similarity": {
                    "applicable": False,
                    "reason": "SIMILAR_TO generation requires full or moderate mode",
                },
                "semantic_edges": {
                    "applicable": False,
                    "reason": "SEMANTICALLY_RELATED generation requires full or moderate mode",
                },
                "git_history": {"applicable": True, "reason": "available in fast mode"},
                "http_links": {"applicable": True, "reason": "available in fast mode"},
                "dependencies": {"applicable": True, "reason": "available in fast mode"},
            },
        )
        self.assertTrue(
            all(
                value["applicable"]
                for value in BENCHMARK.index_mode_capability_applicability("full").values()
            )
        )

    def test_index_tool_arguments_preserve_requested_mode(self) -> None:
        self.assertEqual(
            BENCHMARK.index_tool_arguments(Path("/tmp/repo"), "moderate"),
            {"repo_path": "/tmp/repo", "mode": "moderate"},
        )
        with self.assertRaisesRegex(ValueError, "unsupported index mode"):
            BENCHMARK.index_tool_arguments(Path("/tmp/repo"), "turbo")

    def test_graded_relevance_scores_mrr_hits_and_ndcg(self) -> None:
        ranked = [
            {"name": "related_helper"},
            {"name": "canonical_entry_point"},
            {"name": "unrelated"},
        ]
        judgments = [
            {"expected_substring": "canonical_entry_point", "relevance": 3},
            {"expected_substring": "related_helper", "relevance": 1},
        ]

        score = BENCHMARK.score_ranked_relevance(ranked, judgments, cutoff=5)

        expected_dcg = 1.0 + 7.0 / BENCHMARK.math.log2(3)
        expected_idcg = 7.0 + 1.0 / BENCHMARK.math.log2(3)
        self.assertEqual(score["first_relevant_rank"], 1)
        self.assertEqual(score["reciprocal_rank"], 1.0)
        self.assertTrue(score["hit_at_1"])
        self.assertTrue(score["hit_at_5"])
        self.assertAlmostEqual(score["ndcg_at_5"], expected_dcg / expected_idcg)
        self.assertEqual(score["matched_relevance"], [1, 3, 0])

    def test_graded_relevance_missing_evidence_scores_zero(self) -> None:
        score = BENCHMARK.score_ranked_relevance(
            [{"name": "unrelated"}],
            [{"expected_substring": "required", "relevance": 3}],
            cutoff=5,
        )

        self.assertIsNone(score["first_relevant_rank"])
        self.assertEqual(score["reciprocal_rank"], 0.0)
        self.assertEqual(score["ndcg_at_5"], 0.0)

    def test_graded_relevance_requires_provenance_on_the_same_result(self) -> None:
        ranked = [
            {
                "name": "canonicalDependencyAPI",
                "source": "project",
                "package": "cbmbenchdep",
                "read_only": False,
            },
            {
                "name": "canonicalDependencyAPI",
                "source": "dependency",
                "package": "cbmbenchdep",
                "read_only": True,
            },
        ]
        judgments = [
            {
                "expected_substring": "canonicalDependencyAPI",
                "required_substrings": [
                    '\"source\":\"dependency\"',
                    '\"package\":\"cbmbenchdep\"',
                    '\"read_only\":true',
                ],
                "relevance": 3,
            }
        ]

        score = BENCHMARK.score_ranked_relevance(ranked, judgments, cutoff=5)

        self.assertEqual(score["first_relevant_rank"], 2)
        self.assertEqual(score["matched_relevance"], [0, 3])

    def test_quality_oracle_accepts_graded_relevance_judgments(self) -> None:
        oracles = {
            "ranked": {
                "response": {
                    "results": [
                        {"name": "related_helper"},
                        {"name": "canonical_entry_point"},
                    ]
                }
            }
        }
        expectations = {
            "ranked": {
                "criterion": "architectural entry points rank ahead of unrelated symbols",
                "judgments": [
                    {"expected_substring": "canonical_entry_point", "relevance": 3},
                    {"expected_substring": "related_helper", "relevance": 1},
                ],
                "cutoff": 5,
            }
        }

        summary = BENCHMARK.score_quality_oracles(oracles, expectations)

        self.assertTrue(summary["passed"])
        self.assertIsNotNone(summary["mean_ndcg_at_5"])
        self.assertEqual(oracles["ranked"]["quality"]["relevance_judgments"], 2)
        self.assertEqual(oracles["ranked"]["quality"]["rank"], 1)
        self.assertIn("ndcg_at_5", oracles["ranked"]["quality"])

    def test_surface_parity_separates_pre_reveal_discovery_from_dispatch(self) -> None:
        schema_a = {"type": "object", "properties": {"query": {"type": "string"}}}
        schema_b = {"type": "object", "properties": {"path": {"type": "string"}}}
        classic = [
            {"name": "search_graph", "inputSchema": schema_a},
            {"name": "index_repository", "inputSchema": schema_b},
            {"name": "get_code_snippet", "inputSchema": schema_b},
        ]
        pre = [
            {"name": "search_graph", "inputSchema": schema_a},
            {
                "name": "get_code",
                "inputSchema": {
                    "type": "object",
                    "properties": {"qualified_name": {"type": "string"}},
                },
            },
            {"name": "_hidden_tools", "inputSchema": {"type": "object"}},
        ]
        post = [
            *pre,
            {"name": "index_repository", "inputSchema": schema_b},
            {"name": "get_code_snippet", "inputSchema": schema_b},
        ]

        comparison = BENCHMARK.compare_mcp_tool_surfaces(
            pre,
            post,
            classic,
            pre_dispatch={
                "search_graph": True,
                "index_repository": True,
                "get_code_snippet": True,
            },
            list_changed_observed=True,
        )

        self.assertEqual(comparison["pre_reveal"]["advertised_classic_tools"], "1/3")
        self.assertEqual(
            comparison["pre_reveal"]["dispatch_recognized_classic_tools"], "3/3"
        )
        self.assertEqual(
            comparison["pre_reveal"]["intentionally_hidden_classic_tools"],
            ["get_code_snippet", "index_repository"],
        )
        self.assertTrue(comparison["pre_reveal"]["classic_dispatch_parity"])
        self.assertFalse(comparison["pre_reveal"]["get_code_alias"]["schema_equal"])
        self.assertFalse(
            comparison["pre_reveal"]["get_code_alias"]["property_names_equal"]
        )
        self.assertEqual(
            comparison["pre_reveal"]["get_code_alias"]["classic_only_properties"],
            ["path"],
        )
        self.assertTrue(comparison["post_reveal"]["classic_name_parity"])
        self.assertTrue(comparison["post_reveal"]["classic_schema_parity"])
        self.assertTrue(comparison["post_reveal"]["tools_list_changed_observed"])
        self.assertTrue(comparison["passed"])

    def test_surface_parity_rejects_post_reveal_schema_drift(self) -> None:
        classic = [
            {"name": "search_graph", "inputSchema": {"type": "object"}},
        ]
        post = [
            {
                "name": "search_graph",
                "inputSchema": {"type": "object", "required": ["query"]},
            },
        ]

        comparison = BENCHMARK.compare_mcp_tool_surfaces(
            classic,
            post,
            classic,
            pre_dispatch={"search_graph": True},
            list_changed_observed=True,
        )

        self.assertFalse(comparison["post_reveal"]["classic_schema_parity"])
        self.assertEqual(
            comparison["post_reveal"]["schema_mismatches"], ["search_graph"]
        )
        self.assertFalse(comparison["passed"])

    def test_explicit_config_override_takes_priority_over_profile(self) -> None:
        overrides = BENCHMARK.resolve_config_overrides(
            "minimal_indexing", ["rank_enabled=true", "auto_index_deps=true"]
        )
        self.assertEqual(overrides["rank_enabled"], "true")
        self.assertEqual(overrides["auto_index_deps"], "true")
        self.assertEqual(overrides["semantic_edges_enabled"], "false")

    def test_benchmark_environment_retains_worker_measurement_log(self) -> None:
        env = BENCHMARK.build_env(Path("/tmp/cbm-benchmark-cache"))
        self.assertEqual(env["CBM_PROFILE"], "1")
        self.assertEqual(env["CBM_AUTO_INDEX"], "false")

    def test_tool_result_separates_default_payload_quality_json_and_transport(self) -> None:
        default_payload = b"total: 1\nresults[1]{name}:\n  alpha\n"
        result = BENCHMARK.build_tool_call_result(
            {"name": "alpha", "items": [1, 2]}, "", 999, 12.5, False,
            default_payload,
        )
        canonical = b'{"items":[1,2],"name":"alpha"}'
        self.assertEqual(result["transport_response_bytes"], 999)
        self.assertEqual(result["response_bytes"], len(default_payload))
        self.assertEqual(result["quality_response_bytes"], len(canonical))
        self.assertEqual(
            result["response_token_estimate"], BENCHMARK.estimate_response_tokens(default_payload)
        )
        self.assertEqual(result["token_estimator"], "utf8_bytes_div_4_ceil")
        self.assertEqual(result["response_encoding"], "tool_default")

    def test_result_text_extractors_preserve_default_toon(self) -> None:
        toon = "total: 1\nresults[1]{name}:\n  alpha\n"
        cli_stdout = '{"content":[{"type":"text","text":"total: 1\\nresults[1]{name}:\\n  alpha\\n"}]}'
        mcp_response = {"result": {"content": [{"type": "text", "text": toon}]}}
        self.assertEqual(BENCHMARK.cli_result_text(cli_stdout), toon)
        self.assertEqual(BENCHMARK.mcp_result_text(mcp_response), toon)

    def test_mcp_tool_call_measures_default_payload_and_uses_json_for_quality(self) -> None:
        class FakeClient:
            def call_tool_text(self, name, arguments):
                self.default_call = (name, arguments)
                return "total: 1\nresults[1]{name}:\n  alpha\n", "default log", 321, 7.25

            def call_tool(self, name, arguments):
                self.quality_call = (name, arguments)
                return {"results": [{"name": "alpha"}]}, "quality log", 654, 2.5

        client = FakeClient()
        result = BENCHMARK.run_mcp_tool_call(
            client, "search_graph", {"name_pattern": "alpha"}, False
        )

        self.assertEqual(
            client.default_call, ("search_graph", {"name_pattern": "alpha"})
        )
        self.assertEqual(
            client.quality_call,
            ("search_graph", {"name_pattern": "alpha", "format": "json"}),
        )
        self.assertEqual(result["elapsed_ms"], 7.25)
        self.assertEqual(result["quality_probe_elapsed_ms"], 2.5)
        self.assertEqual(result["transport_response_bytes"], 321)
        self.assertEqual(result["response_encoding"], "tool_default")
        self.assertEqual(result["response"]["results"][0]["name"], "alpha")

    def test_quality_summary_requires_every_applicable_oracle(self) -> None:
        oracles = {
            "marker_search_graph": {
                "response": {"results": [{"name": "wanted_marker"}]}
            },
            "changed_file_query_graph": {
                "response": {"results": [{"file_path": "wrong.c"}]}
            },
            "route_freshness_probe": {"response": {"routes": []}},
        }
        expectations = {
            "marker_search_graph": ("wanted_marker", "marker returned"),
            "changed_file_query_graph": ("src/wanted.c", "changed path returned"),
            "route_freshness_probe": (None, "route check not applicable"),
        }
        summary = BENCHMARK.score_quality_oracles(oracles, expectations)
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["passed_count"], 1)
        self.assertEqual(summary["applicable_count"], 2)
        self.assertEqual(summary["score"], 0.5)
        self.assertFalse(oracles["changed_file_query_graph"]["quality"]["passed"])
        self.assertFalse(oracles["route_freshness_probe"]["quality"]["applicable"])

    def test_quality_summary_records_rank_and_hit_rates(self) -> None:
        oracles = {
            "ranked": {
                "response": {
                    "results": [
                        {"name": "unrelated"},
                        {"name": "wanted_marker"},
                    ]
                }
            }
        }
        summary = BENCHMARK.score_quality_oracles(
            oracles, {"ranked": ("wanted_marker", "marker is ranked")}
        )
        quality = oracles["ranked"]["quality"]
        self.assertEqual(quality["rank"], 2)
        self.assertFalse(quality["hit_at_1"])
        self.assertTrue(quality["hit_at_5"])
        self.assertEqual(quality["reciprocal_rank"], 0.5)
        self.assertEqual(quality["returned_count"], 2)
        self.assertEqual(summary["mean_reciprocal_rank"], 0.5)
        self.assertEqual(summary["hit_at_1"], 0.0)
        self.assertEqual(summary["hit_at_5"], 1.0)
        self.assertEqual(summary["score"], 0.5)

    def test_binary_metadata_records_content_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "cbm"
            binary.write_bytes(b"auditable-binary")
            metadata = BENCHMARK.binary_metadata(binary)
        self.assertEqual(metadata["size_bytes"], 16)
        self.assertEqual(
            metadata["sha256"],
            "5d984f78de8a55923b5ab343710b12830af15f8415f350135e5346fc7753b4d5",
        )
        self.assertTrue(metadata["path"].endswith("/cbm"))

    def test_build_index_result_reports_maximum_logged_peak_rss(self) -> None:
        stderr = "\n".join(
            (
                "level=info msg=mem.phase phase=registry_build rss_mb=120 peak_mb=144",
                "level=info msg=mem.phase phase=parallel_resolve rss_mb=192 peak_mb=256",
                "level=info msg=pipeline.done elapsed_ms=80",
            )
        )
        result = BENCHMARK.build_index_result(
            {"publish_kind": "full"}, stderr, stdout_bytes=10, elapsed_ms=100.0,
            include_logs=False,
        )
        self.assertEqual(result["peak_rss_mb"], 256)

    def test_build_index_result_reads_final_peak_for_sequential_and_incremental_runs(self) -> None:
        for marker in ("pipeline.done", "incremental.done"):
            with self.subTest(marker=marker):
                result = BENCHMARK.build_index_result(
                    {"publish_kind": "incremental_exact"},
                    f"level=info msg={marker} elapsed_ms=18 rss_mb=42 peak_mb=64",
                    stdout_bytes=10,
                    elapsed_ms=20.0,
                    include_logs=False,
                )
                self.assertEqual(result["peak_rss_mb"], 64)

    def test_build_index_result_reads_bounded_worker_log_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            logfile = Path(tmpdir) / "index.log"
            logfile.write_text(
                "ignored detail\n"
                "level=info msg=mem.phase phase=parallel_resolve rss_mb=192 peak_mb=320\n"
                "level=info msg=pipeline.done elapsed_ms=81\n",
                encoding="utf-8",
            )
            result = BENCHMARK.build_index_result(
                {"publish_kind": "full", "logfile": "/missing/response.log"},
                (
                    "level=info msg=index.supervisor.reap outcome=clean\n"
                    f"level=info msg=index.supervisor.profile_log log={logfile}"
                ),
                stdout_bytes=10,
                elapsed_ms=100.0,
                include_logs=False,
            )

        self.assertEqual(result["peak_rss_mb"], 320)
        self.assertEqual(result["logged_elapsed_ms"]["pipeline_done"], 81)
        self.assertEqual(len(result["measurement_log_markers"]), 2)
        self.assertNotIn("ignored detail", "\n".join(result["measurement_log_markers"]))

    def test_build_index_result_archives_worker_log_before_worktree_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            logfile = root / "index.log"
            artifact_dir = root / "durable-artifacts"
            logfile.write_text(
                "level=info msg=mem.phase phase=parallel_resolve rss_mb=192 peak_mb=320\n",
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ, {BENCHMARK.BENCHMARK_ARTIFACT_DIR_ENV: str(artifact_dir)}
            ):
                result = BENCHMARK.build_index_result(
                    {"publish_kind": "full"},
                    f"level=info msg=index.supervisor.profile_log log={logfile}",
                    stdout_bytes=10,
                    elapsed_ms=100.0,
                    include_logs=False,
                )

            artifact = result["measurement_log_artifacts"][0]
            archived_path = artifact_dir / artifact["artifact_name"]
            logfile.unlink()

            self.assertTrue(archived_path.is_file())
            self.assertIn("msg=mem.phase", gzip.decompress(archived_path.read_bytes()).decode())
            self.assertEqual(artifact["source_name"], "index.log")

    def test_build_index_result_records_dependency_phase_and_package_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            logfile = Path(tmpdir) / "index.log"
            logfile.write_text(
                "level=info msg=prof phase=index_repository "
                "sub=dep_auto_index ms=52347 us=52347915\n",
                encoding="utf-8",
            )
            result = BENCHMARK.build_index_result(
                {"publish_kind": "full", "dependencies_indexed": 6},
                f"level=info msg=index.supervisor.profile_log log={logfile}",
                stdout_bytes=10,
                elapsed_ms=60000.0,
                include_logs=False,
            )

        self.assertEqual(
            result["dependency_indexing"],
            {
                "measurement_status": "measured",
                "phase_elapsed_ms": 52347,
                "packages_indexed": 6,
            },
        )
        self.assertIn("sub=dep_auto_index", "\n".join(result["measurement_log_markers"]))

    def test_build_index_result_attributes_cold_process_overhead_after_worker_total(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            logfile = Path(tmpdir) / "index.log"
            logfile.write_text(
                "level=info msg=pipeline.done elapsed_ms=100\n"
                "level=info msg=prof phase=index_repository sub=dep_auto_index ms=500 us=500000\n"
                "level=info msg=prof phase=index_repository sub=rank_refresh ms=20 us=20000\n"
                "level=info msg=prof phase=index_repository sub=TOTAL ms=650 us=650000\n",
                encoding="utf-8",
            )
            result = BENCHMARK.build_index_result(
                {"publish_kind": "full", "dependencies_indexed": 1},
                f"level=info msg=index.supervisor.profile_log log={logfile}",
                stdout_bytes=10,
                elapsed_ms=2650.0,
                include_logs=False,
            )

        self.assertEqual(result["worker_elapsed_ms"], 650)
        self.assertEqual(result["process_overhead_ms"], 2000)
        self.assertEqual(result["unlogged_overhead_ms"], 2000)
        self.assertEqual(
            result["timing_components_ms"],
            {
                "main_index": 100,
                "dependency_index": 500,
                "rank_refresh": 20,
                "worker_total": 650,
                "cold_process_and_supervisor": 2000,
            },
        )

    def test_build_index_result_marks_uninstrumented_dependency_phase_unknown(self) -> None:
        result = BENCHMARK.build_index_result(
            {"publish_kind": "full"},
            "",
            stdout_bytes=10,
            elapsed_ms=20.0,
            include_logs=False,
        )

        self.assertEqual(
            result["dependency_indexing"],
            {
                "measurement_status": "unknown",
                "phase_elapsed_ms": None,
                "packages_indexed": None,
            },
        )

    def test_route_handler_mutation_adds_executable_route_registration(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            source = repo / "src" / "ui" / "http_server.c"
            source.parent.mkdir(parents=True)
            source.write_text("/* fixture */\n", encoding="utf-8")

            mutation = BENCHMARK.mutate_self_dogfood_scenario("route_handler", repo)
            mutated = source.read_text(encoding="utf-8")

        self.assertEqual(mutation["changed_paths"], ["src/ui/http_server.c"])
        self.assertIn("cbm_pan4_oracle_route_handler", mutated)
        self.assertIn('cbm_http_path_match(path, "/api/pan4-oracle")', mutated)
        self.assertNotIn("route oracle literal", mutated)

    def test_c_new_leaf_mutation_adds_hashed_indexed_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            source = repo / "src" / "cbm_benchmark_leaf.c"
            mutation = BENCHMARK.mutate_self_dogfood_scenario("c_new_leaf", repo)
            mutated = source.read_text(encoding="utf-8")
            source_sha256 = BENCHMARK.file_sha256(source)

        self.assertEqual(mutation["changed_paths"], ["src/cbm_benchmark_leaf.c"])
        self.assertEqual(mutation["description"], "new isolated C source file")
        self.assertIn("static int cbm_pan4_oracle_c_new_leaf(void)", mutated)
        self.assertEqual(
            mutation["source_hashes"],
            [
                {
                    "path": "src/cbm_benchmark_leaf.c",
                    "before_sha256": None,
                    "after_sha256": source_sha256,
                }
            ],
        )

    def test_self_dogfood_worktree_uses_the_declared_revision(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_repo = Path(tmpdir) / "source" / "repo"
            case_root = Path(tmpdir) / "campaign" / "cell"
            completed = subprocess.CompletedProcess([], 0, "", "")

            with mock.patch.object(
                BENCHMARK, "command_result", return_value=(completed, 1)
            ) as run:
                repo_dir = BENCHMARK.create_self_dogfood_worktree(
                    source_repo,
                    case_root,
                    30,
                    "a" * 40,
                )

            self.assertEqual(repo_dir, case_root / BENCHMARK.SELF_DOGFOOD_REPO_SUBDIR)
            self.assertEqual(
                run.call_args.args[0],
                [
                    "git",
                    "worktree",
                    "add",
                    "--detach",
                    str(repo_dir),
                    "a" * 40,
                ],
            )

    def test_build_index_result_uses_none_without_memory_markers(self) -> None:
        result = BENCHMARK.build_index_result(
            {"publish_kind": "full"}, "level=info msg=pipeline.done elapsed_ms=80", 10,
            100.0, False,
        )
        self.assertIsNone(result["peak_rss_mb"])


if __name__ == "__main__":
    unittest.main()
