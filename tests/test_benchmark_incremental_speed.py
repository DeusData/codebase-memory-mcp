import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark-incremental-speed.py"
SPEC = importlib.util.spec_from_file_location("benchmark_incremental_speed", SCRIPT)
assert SPEC and SPEC.loader
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


class BenchmarkIncrementalSpeedTest(unittest.TestCase):
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

    def test_build_index_result_uses_none_without_memory_markers(self) -> None:
        result = BENCHMARK.build_index_result(
            {"publish_kind": "full"}, "level=info msg=pipeline.done elapsed_ms=80", 10,
            100.0, False,
        )
        self.assertIsNone(result["peak_rss_mb"])


if __name__ == "__main__":
    unittest.main()
