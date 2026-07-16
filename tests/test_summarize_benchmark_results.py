import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "summarize-benchmark-results.py"
SPEC = importlib.util.spec_from_file_location("summarize_benchmark_results", SCRIPT)
assert SPEC and SPEC.loader
SUMMARY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARY)


def report(case: dict, sha: str = "a" * 64) -> dict:
    return {
        "binary_metadata": {"sha256": sha, "size_bytes": 123},
        "parameters": {
            "config_profile": "rank_disabled",
            "config_overrides": {"rank_enabled": "false"},
        },
        "cleanup": {"requested": True, "removed": True},
        "cases": [case],
    }


class SummarizeBenchmarkResultsTest(unittest.TestCase):
    def test_composition_spec_groups_validated_campaign_cells(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            campaign_root = root / "campaign"
            campaign_root.mkdir()
            matrix_spec = root / "matrix.json"
            matrix_spec.write_text('{"schema_version": 1}\n', encoding="utf-8")
            for label in ("rank", "incremental"):
                (campaign_root / f"{label}.json").write_text(
                    json.dumps({"binary_metadata": {"sha256": "a" * 64}, "cases": []}),
                    encoding="utf-8",
                )
            composition = root / "composition.json"
            composition.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaigns": {
                            "fixture": {
                                "matrix_spec": "matrix.json",
                                "campaign_root": "campaign",
                            }
                        },
                        "groups": [
                            {
                                "label": "latest-default-mcp",
                                "inputs": [
                                    {
                                        "campaign": "fixture",
                                        "cell_labels": ["rank", "incremental"],
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            class FakeCampaign:
                @staticmethod
                def expand_matrix_spec(spec: dict) -> dict:
                    self.assertEqual(spec["schema_version"], 1)
                    return {"cells": [{"label": "rank"}, {"label": "incremental"}]}

                @staticmethod
                def completed_report_inputs(
                    path: Path, cells: list[dict]
                ) -> list[tuple[str, Path]]:
                    return [
                        (cell["label"], path / f"{cell['label']}.json")
                        for cell in cells
                    ]

            grouped, provenance = SUMMARY.load_composition_groups(
                composition, FakeCampaign
            )

            self.assertEqual(len(grouped["latest-default-mcp"]), 2)
            self.assertEqual(provenance["input_count"], 2)
            self.assertEqual(provenance["spec_path"], str(composition.resolve()))

    def test_search_projection_report_keeps_identity_quality_beside_size(self) -> None:
        document = {
            "mode": "search_projection",
            "run_id": "projection-example",
            "binary_metadata": {"sha256": "b" * 64},
            "cleanup": {"removed": True},
            "completion": {"status": "complete", "exit_code": 0},
            "observations": [
                {
                    "variant": "compact_true",
                    "returned_count": 30,
                    "identity_equal_to_default": True,
                    "property_fields": [],
                    "internal_fields": [],
                    "response_bytes": 6824,
                    "response_token_estimate": 1706,
                    "elapsed_ms": 10.0,
                    "post_call_rss_kb": 13696,
                    "transport_survived": True,
                    "server_reaped": True,
                },
                {
                    "variant": "compact_false",
                    "returned_count": 30,
                    "identity_equal_to_default": True,
                    "property_fields": ["complexity", "signature"],
                    "internal_fields": [],
                    "response_bytes": 18374,
                    "response_token_estimate": 4594,
                    "elapsed_ms": 8.0,
                    "post_call_rss_kb": 14000,
                    "transport_survived": True,
                    "server_reaped": True,
                },
            ],
            "derived": {
                "passed": True,
                "identity_parity": True,
                "internal_fields_absent": True,
                "compact_bytes": 6824,
                "non_compact_bytes": 18374,
                "non_compact_over_compact_ratio": 2.693,
                "claim_boundary": "One observation per variant.",
            },
        }

        markdown = SUMMARY.render_search_projection(document)

        self.assertIn("Outcome: complete", markdown)
        self.assertIn("compact true | 30 | Equal | none | 6,824 | 1,706", markdown)
        self.assertIn("non-compact | 30 | Equal | complexity, signature | 18,374", markdown)
        self.assertIn("62.9% fewer payload bytes", markdown)
        self.assertIn("No fp, sp, or bt", markdown)
        self.assertIn("not a latency comparison", markdown)
        self.assertNotIn("FAIL", markdown)

    def test_search_projection_rejects_wrong_document(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected a search_projection"):
            SUMMARY.render_search_projection({"mode": "incremental"})

    def test_list_projects_scaling_report_separates_size_latency_and_lifecycle(self) -> None:
        document = {
            "mode": "list_projects_scaling",
            "run_id": "list-projects-example",
            "binary_metadata": {"sha256": "a" * 64},
            "cleanup": {"requested": True, "removed": True},
            "completion": {"status": "complete", "exit_code": 0},
            "parameters": {
                "token_estimator": "utf8_bytes_div_4_ceil",
                "rss_measurement": "post_call_resident_kb_not_peak",
            },
            "observations": [
                {
                    "requested_projects": 1,
                    "returned_projects": 1,
                    "response_bytes": 284,
                    "response_token_estimate": 71,
                    "elapsed_ms": 15.149,
                    "post_call_rss_kb": 16576,
                    "transport_survived": True,
                    "server_reaped": True,
                    "fixture_db_bytes": 6160384,
                    "passed": True,
                },
                {
                    "requested_projects": 64,
                    "returned_projects": 64,
                    "response_bytes": 12695,
                    "response_token_estimate": 3174,
                    "elapsed_ms": 82.118,
                    "post_call_rss_kb": 12784,
                    "transport_survived": True,
                    "server_reaped": True,
                    "fixture_db_bytes": 394264576,
                    "passed": True,
                },
            ],
            "derived": {
                "passed": True,
                "incremental_response_bytes_per_project": 197.0,
                "claim_boundary": "Measures list_projects alone; not combined calls.",
            },
        }

        markdown = SUMMARY.render_list_projects_scaling(document)

        self.assertIn("Outcome: complete", markdown)
        self.assertIn("64 | 64 | 12,695 | 3,174 | 82.118", markdown)
        self.assertIn("Survived | Reaped", markdown)
        self.assertIn("197.0 bytes per added project", markdown)
        self.assertIn("one observation per project count", markdown)
        self.assertIn("not peak RSS", markdown)
        self.assertNotIn("FAIL", markdown)

    def test_list_projects_scaling_rejects_wrong_document(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected a list_projects_scaling"):
            SUMMARY.render_list_projects_scaling({"mode": "incremental"})

    def test_mcp_surface_report_keeps_discovery_dispatch_and_behavior_distinct(self) -> None:
        def surface(count: int, size: int, tokens: int, elapsed: float) -> dict:
            return {
                "tool_count": count,
                "response_bytes": size,
                "response_token_estimate": tokens,
                "list_elapsed_ms": elapsed,
            }

        document = {
            "mode": "mcp_surface_parity",
            "surfaces": {
                "classic": surface(15, 21000, 5250, 0.4),
                "streamlined_pre_reveal": surface(6, 13000, 3250, 0.3),
                "streamlined_post_reveal": surface(17, 24000, 6000, 0.2),
            },
            "comparison": {
                "pre_reveal": {
                    "advertised_classic_tools": "4/15",
                    "dispatch_recognized_classic_tools": "15/15",
                    "intentionally_hidden_classic_tools": ["index_repository"],
                    "get_code_alias": {
                        "property_names_equal": True,
                        "required_names_equal": True,
                        "schema_equal": False,
                    },
                },
                "post_reveal": {
                    "classic_name_parity": True,
                    "classic_schema_parity": True,
                    "tools_list_changed_observed": True,
                },
            },
        }

        markdown = SUMMARY.render_mcp_surface_parity(document)

        self.assertIn("Pure classic | 15 | 15/15", markdown)
        self.assertIn("Streamlined before reveal | 6 | 4/15 | 15/15", markdown)
        self.assertIn("Same streamlined process after reveal | 17 | 15/15", markdown)
        self.assertIn("does not prove successful execution", markdown)
        self.assertIn("Behavioral parity requires capability fixtures", markdown)
        self.assertIn("full schema equal=false", markdown)

    def test_mcp_surface_report_rejects_regular_benchmark_document(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected an mcp_surface_parity"):
            SUMMARY.render_mcp_surface_parity({"mode": "incremental"})

    def test_rank_beyond_cutoff_is_not_reported_as_missing(self) -> None:
        case = {
            "passed": False,
            "oracles": {
                "passed": False,
                "quality": {"passed": False, "passed_count": 0, "applicable_count": 1},
                "central_order_search": {
                    "quality": {
                        "applicable": True,
                        "passed": False,
                        "criterion": "central result appears by rank five",
                        "expected_substring": "zz_order_core",
                        "rank": 9,
                        "returned_count": 9,
                        "reciprocal_rank": 1 / 9,
                        "hit_at_1": False,
                        "hit_at_5": False,
                        "ndcg_at_5": 0.0,
                    }
                },
            },
        }

        details = SUMMARY.quality_oracle_details([case])

        self.assertEqual(details[0]["result"], "BELOW CUTOFF (rank 9 of 9)")

    def test_capability_quality_shortfall_is_not_called_correctness_failure(self) -> None:
        item = report(
            {
                "scenario": "rank_quality",
                "passed": True,
                "execution_passed": True,
                "quality_target_met": False,
                "oracles": {
                    "passed": False,
                    "quality": {
                        "passed": False,
                        "passed_count": 0,
                        "applicable_count": 1,
                        "score": 1 / 9,
                    },
                    "central_order_search": {
                        "quality": {
                            "applicable": True,
                            "passed": False,
                            "expected_substring": "zz_order_core",
                            "rank": 9,
                            "returned_count": 9,
                            "relevance_cutoff": 5,
                        }
                    },
                },
            }
        )
        item["mode"] = "capability_quality"

        row = SUMMARY.summarize_group("rank-disabled", [item])

        self.assertEqual(row["decision"], "BELOW QUALITY TARGET")
        self.assertIn("below quality cutoff (rank 9, cutoff 5)", row["findings"][0])
        self.assertNotIn("failed", row["findings"][0])

    def test_report_aggregates_graded_ndcg_without_hiding_mrr(self) -> None:
        case = {
            "scenario": "rank_quality",
            "passed": True,
            "canonical_graph": {"equal": True},
            "oracles": {
                "passed": True,
                "quality": {
                    "passed": True,
                    "passed_count": 1,
                    "applicable_count": 1,
                    "score": 0.5,
                    "hit_at_1": 0.0,
                    "hit_at_5": 1.0,
                    "mean_ndcg_at_5": 0.8,
                    "ndcg_applicable_count": 1,
                },
                "ranked_probe": {
                    "quality": {
                        "applicable": True,
                        "passed": True,
                        "criterion": "graded architectural relevance",
                        "expected_substring": "entry_point",
                        "rank": 2,
                        "returned_count": 5,
                        "reciprocal_rank": 0.5,
                        "hit_at_1": False,
                        "hit_at_5": True,
                        "relevance_judgments": 3,
                        "ndcg_at_5": 0.8,
                    }
                },
            },
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100},
        }

        row = SUMMARY.summarize_group("rank-on", [report(case)])
        markdown = SUMMARY.render_markdown([row])

        self.assertEqual(row["quality_score"], 0.5)
        self.assertEqual(row["ndcg_at_5"], 0.8)
        self.assertIn("nDCG@5", markdown)
        self.assertIn("0.800", markdown)
        self.assertIn("3 judgments", markdown)
        self.assertIn("doi.org/10.1145/582415.582418", markdown)

    def test_fast_mode_report_marks_similarity_and_semantic_quality_not_applicable(self) -> None:
        case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "incremental": {"elapsed_ms": 10},
            "fresh_fast_full_after_change": {"elapsed_ms": 100},
        }
        item = report(case)
        item["parameters"].update(
            {
                "index_mode": "fast",
                "capability_applicability": {
                    "rank": {"applicable": True, "reason": "available in fast mode"},
                    "similarity": {
                        "applicable": False,
                        "reason": "SIMILAR_TO generation requires full or moderate mode",
                    },
                    "semantic_edges": {
                        "applicable": False,
                        "reason": "SEMANTICALLY_RELATED generation requires full or moderate mode",
                    },
                },
            }
        )

        row = SUMMARY.summarize_group("fast", [item])
        markdown = SUMMARY.render_markdown([row])

        self.assertEqual(row["index_modes"], "fast")
        self.assertEqual(
            row["capability_applicability"]["similarity"],
            "N/A: SIMILAR_TO generation requires full or moderate mode",
        )
        self.assertIn("## Algorithm-quality applicability", markdown)
        self.assertIn("N/A: SIMILAR_TO generation requires full or moderate mode", markdown)

    def test_candidate_support_overrides_mode_based_applicability(self) -> None:
        item = report({"passed": True})
        item["parameters"].update(
            {
                "index_mode": "fast",
                "capability_applicability": {
                    "rank": {"applicable": True, "reason": "available in fast mode"},
                    "dependencies": {
                        "applicable": True,
                        "reason": "available in fast mode",
                    },
                },
                "capability_support": {"rank": False, "dependencies": False},
            }
        )

        row = SUMMARY.summarize_group("upstream", [item])

        self.assertEqual(row["capability_applicability"]["rank"], "unsupported by candidate")
        self.assertEqual(
            row["capability_applicability"]["dependencies"], "unsupported by candidate"
        )
        self.assertEqual(row["dependency_mode"], "unsupported")

    def test_dependency_breakdown_reads_new_and_retained_result_shapes(self) -> None:
        new_case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "initial_fast_full": {
                "dependency_indexing": {
                    "measurement_status": "measured",
                    "phase_elapsed_ms": 120,
                    "packages_indexed": 6,
                }
            },
            "incremental": {
                "elapsed_ms": 10,
                "measurement_log_markers": [
                    "level=info msg=prof phase=index_repository "
                    "sub=dep_auto_index ms=4 us=4000"
                ],
                "response": {"dependencies_indexed": 2},
            },
            "fresh_fast_full_after_change": {
                "elapsed_ms": 100,
                "measurement_log_markers": [
                    "level=info msg=prof phase=index_repository "
                    "sub=dep_auto_index ms=80 us=80000"
                ],
                "response": {"dependencies_indexed": 6},
            },
        }
        item = report(new_case)
        item["parameters"]["config_profile"] = "default"
        item["parameters"]["config_overrides"] = {}

        row = SUMMARY.summarize_group("latest-default", [item])

        self.assertEqual(row["dependency_mode"], "enabled (observed)")
        self.assertEqual(row["dependency_packages_p50"], 6.0)
        self.assertEqual(row["dependency_initial_p50_ms"], 120.0)
        self.assertEqual(row["dependency_incremental_p50_ms"], 4.0)
        self.assertEqual(row["dependency_fresh_p50_ms"], 80.0)

    def test_capability_quality_reports_initial_full_time_and_peak_rss(self) -> None:
        case = {
            "passed": True,
            "quality_target_met": True,
            "initial_fast_full": {"elapsed_ms": 125, "peak_rss_mb": 42},
        }
        item = report(case)
        item["mode"] = "capability_quality"

        row = SUMMARY.summarize_group("quality", [item])

        self.assertEqual(row["full_p50_ms"], 125.0)
        self.assertEqual(row["full_observations"], 1)
        self.assertEqual(row["peak_rss_mb"], 42)

    def test_dependency_mode_distinguishes_disabled_unsupported_and_unknown(self) -> None:
        case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "incremental": {"elapsed_ms": 10},
            "fresh_fast_full_after_change": {"elapsed_ms": 100},
        }
        disabled = report(case)
        disabled["parameters"]["config_profile"] = "dependency_disabled"
        disabled["parameters"]["config_overrides"] = {"auto_index_deps": "false"}
        unsupported = report(case)
        unsupported["parameters"]["capability_support"] = {"auto_index_deps": False}
        unknown = report(case)
        unknown["parameters"]["config_overrides"] = {}

        self.assertEqual(
            SUMMARY.summarize_group("disabled", [disabled])["dependency_mode"],
            "disabled (explicit)",
        )
        self.assertEqual(
            SUMMARY.summarize_group("upstream", [unsupported])["dependency_mode"],
            "unsupported",
        )
        self.assertEqual(
            SUMMARY.summarize_group("historical", [unknown])["dependency_mode"],
            "unknown",
        )

    def test_markdown_reports_dependency_cost_and_methodology_sources(self) -> None:
        case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "incremental": {
                "elapsed_ms": 10,
                "dependency_indexing": {
                    "measurement_status": "measured",
                    "phase_elapsed_ms": 3,
                    "packages_indexed": 1,
                },
            },
            "fresh_fast_full_after_change": {"elapsed_ms": 100},
        }
        item = report(case)
        item["parameters"]["config_overrides"] = {"auto_index_deps": "true"}

        markdown = SUMMARY.render_markdown([SUMMARY.summarize_group("deps-on", [item])])

        self.assertIn("## Dependency-indexing capability and cost", markdown)
        self.assertIn("enabled (explicit)", markdown)
        self.assertIn("trec.nist.gov/data/qa.html", markdown)
        self.assertIn("doi.org/10.1145/582415.582418", markdown)
        self.assertIn("arxiv.org/abs/2007.10899", markdown)
        self.assertIn("doi.org/10.1109/4235.996017", markdown)
        self.assertIn("confidence interval", markdown)

    def test_quality_failure_blocks_acceptance_even_with_high_speedup(self) -> None:
        case = {
            "passed": False,
            "canonical_graph": {
                "equal": False,
                "kind": "canonical nodes",
                "left_count": 10,
                "right_count": 11,
                "left_only": "Function\told_value",
                "right_only": "Function\tnew_value",
            },
            "oracles": {
                "passed": True,
                "route": {
                    "quality": {
                        "passed": False,
                        "expected_substring": "/api/pan4-oracle",
                    }
                },
            },
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 90},
            "speedup_full_rebuild_over_incremental": 10.0,
        }
        row = SUMMARY.summarize_group("latest-rank-off", [report(case)])
        self.assertEqual(row["decision"], "REJECT: graph correctness")
        self.assertEqual(row["canonical"], "0/1")
        self.assertEqual(row["speedup_p50"], 10.0)
        self.assertEqual(
            row["findings"],
            [
                "canonical nodes mismatch (incremental=10, fresh=11); "
                "witness: Function old_value vs Function new_value",
                "route failed (expected /api/pan4-oracle)",
            ],
        )

    def test_aggregate_reports_p50_p95_peak_rss_and_cleanup(self) -> None:
        reports = []
        for elapsed, speedup, peak in ((10, 10.0, 90), (20, 5.0, 110), (30, 3.0, 100)):
            case = {
                "passed": True,
                "canonical_graph": {"equal": True},
                "oracles": {"passed": True},
                "incremental": {"elapsed_ms": elapsed, "peak_rss_mb": peak},
                "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 120},
                "speedup_full_rebuild_over_incremental": speedup,
            }
            reports.append(report(case))
        row = SUMMARY.summarize_group("latest-rank-off", reports)
        self.assertEqual(row["incremental_p50_ms"], 20.0)
        self.assertEqual(row["incremental_p95_ms"], 30.0)
        self.assertEqual(row["peak_rss_mb"], 120)
        self.assertEqual(row["lifecycle"], "disposed 3/3")
        self.assertEqual(row["decision"], "PASS")

    def test_lifecycle_distinguishes_retained_evidence_from_cleanup_failure(self) -> None:
        case = {"passed": True}
        retained = report(case)
        retained["cleanup"] = {"requested": False, "removed": False}
        failed = report(case)
        failed["cleanup"] = {"requested": True, "removed": False}

        retained_row = SUMMARY.summarize_group("retained", [retained])
        failed_row = SUMMARY.summarize_group("failed", [failed])

        self.assertEqual(retained_row["lifecycle"], "retained by request 1/1")
        self.assertEqual(failed_row["lifecycle"], "CLEANUP FAILED 1/1")
        markdown = SUMMARY.render_markdown([retained_row, failed_row])
        self.assertIn("Evidence lifecycle", markdown)
        self.assertNotIn("Cleanup |", markdown)

    def test_markdown_places_quality_before_performance(self) -> None:
        case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 90},
            "speedup_full_rebuild_over_incremental": 10.0,
        }
        markdown = SUMMARY.render_markdown([SUMMARY.summarize_group("latest", [report(case)])])
        self.assertLess(markdown.index("Decision"), markdown.index("Speedup p50"))
        self.assertIn("Binary SHA-256", markdown)
        self.assertIn("Correctness and quality findings", markdown)
        self.assertIn("exact default tool-response payload", markdown)
        self.assertIn("consult Cases and the immutable campaign manifest", markdown)

    def test_query_quality_size_latency_and_pareto_frontier(self) -> None:
        compact_case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "oracles": {
                "quality": {
                    "passed": True,
                    "passed_count": 2,
                    "applicable_count": 2,
                    "score": 0.75,
                    "hit_at_1": 0.5,
                    "hit_at_5": 1.0,
                },
                "marker": {
                    "elapsed_ms": 5,
                    "response_bytes": 80,
                    "response_token_estimate": 20,
                },
            },
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 90},
            "speedup_full_rebuild_over_incremental": 10.0,
        }
        slower_case = {
            **compact_case,
            "oracles": {
                "quality": {
                    "passed": True,
                    "passed_count": 2,
                    "applicable_count": 2,
                    "score": 0.75,
                    "hit_at_1": 0.5,
                    "hit_at_5": 1.0,
                },
                "marker": {
                    "elapsed_ms": 8,
                    "response_bytes": 120,
                    "response_token_estimate": 30,
                },
            },
            "incremental": {"elapsed_ms": 20, "peak_rss_mb": 100},
        }
        rows = [
            SUMMARY.summarize_group("compact", [report(compact_case)]),
            SUMMARY.summarize_group("slower", [report(slower_case)]),
        ]
        SUMMARY.mark_pareto_frontier(rows)
        self.assertEqual(rows[0]["quality_score"], 0.75)
        self.assertAlmostEqual(rows[0]["overall_quality_score"], 0.75 ** (1 / 3))
        self.assertEqual(rows[0]["graph_fidelity_score"], 1.0)
        self.assertEqual(rows[0]["task_success_score"], 1.0)
        self.assertEqual(rows[0]["hit_at_1"], 0.5)
        self.assertEqual(rows[0]["hit_at_5"], 1.0)
        self.assertEqual(rows[0]["query_response_p50_bytes"], 80.0)
        self.assertEqual(rows[0]["query_response_p50_tokens"], 20.0)
        self.assertEqual(rows[0]["query_latency_p50_ms"], 5.0)
        self.assertEqual(rows[0]["pareto"], "frontier")
        self.assertEqual(rows[1]["pareto"], "dominated by compact")

    def test_markdown_names_oracles_and_explains_quality_categories(self) -> None:
        case = {
            "scenario": "route_handler",
            "passed": True,
            "canonical_graph": {"equal": True},
            "oracles": {
                "passed": True,
                "quality": {
                    "passed": True,
                    "passed_count": 1,
                    "applicable_count": 1,
                    "score": 0.5,
                    "hit_at_1": 0.0,
                    "hit_at_5": 1.0,
                },
                "route_freshness_probe": {
                    "elapsed_ms": 3,
                    "response_bytes": 20,
                    "response_token_estimate": 5,
                    "quality": {
                        "applicable": True,
                        "passed": True,
                        "criterion": "new route literal appears in route search",
                        "expected_substring": "/api/pan4-oracle",
                        "rank": 2,
                        "returned_count": 5,
                        "reciprocal_rank": 0.5,
                        "hit_at_1": False,
                        "hit_at_5": True,
                    },
                },
                "not_applicable_probe": {
                    "quality": {
                        "applicable": False,
                        "passed": None,
                        "criterion": "not applicable to this mutation",
                    }
                },
            },
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 90},
        }
        markdown = SUMMARY.render_markdown([SUMMARY.summarize_group("rank-off", [report(case)])])
        self.assertIn("Overall quality", markdown)
        self.assertIn("Retrieval MRR", markdown)
        self.assertIn("Graph fidelity", markdown)
        self.assertIn("Task success", markdown)
        self.assertIn("route_freshness_probe", markdown)
        self.assertIn("new route literal appears in route search", markdown)
        self.assertIn("PASS (rank 2 of 5)", markdown)
        self.assertIn("N/A", markdown)
        self.assertIn("geometric mean", markdown)
        self.assertIn("trec.nist.gov", markdown)
        self.assertIn("rank_disabled", markdown)

    def test_partial_probe_success_remains_visible_beside_hard_rejection(self) -> None:
        case = {
            "passed": False,
            "canonical_graph": {"equal": True},
            "oracles": {
                "passed": False,
                "quality": {
                    "passed": False,
                    "passed_count": 4,
                    "applicable_count": 5,
                    "score": 0.7,
                    "hit_at_1": 0.6,
                    "hit_at_5": 0.8,
                },
            },
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 90},
        }
        row = SUMMARY.summarize_group("partial", [report(case)])
        self.assertEqual(row["decision"], "REJECT: task correctness")
        self.assertEqual(row["task_success_score"], 0.8)
        self.assertAlmostEqual(row["overall_quality_score"], (0.7 * 1.0 * 0.8) ** (1 / 3))
        markdown = SUMMARY.render_markdown([row])
        self.assertIn("0.800", markdown)
        self.assertIn("4/5 / 1/1 / 0/1", markdown)

    def test_composed_disabled_capability_is_below_target_not_correctness_rejection(
        self,
    ) -> None:
        quality_report = report(
            {
                "passed": False,
                "execution_passed": True,
                "quality_target_met": False,
                "fixture": {"capability": "rank"},
                "oracles": {
                    "passed": False,
                    "quality": {
                        "passed": False,
                        "passed_count": 0,
                        "applicable_count": 1,
                        "score": 0.1,
                    },
                },
            }
        )
        quality_report["mode"] = "capability_quality"
        incremental_report = report(
            {
                "passed": True,
                "canonical_graph": {"equal": True},
                "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            }
        )
        incremental_report["mode"] = "matrix"

        row = SUMMARY.summarize_group(
            "rank-disabled", [quality_report, incremental_report]
        )

        self.assertEqual(row["decision"], "BELOW QUALITY TARGET")

    def test_pareto_reason_lists_missing_axes_for_ineligible_row(self) -> None:
        row = SUMMARY.summarize_group(
            "incomplete",
            [report({"passed": True, "canonical_graph": {"equal": True}})],
        )
        SUMMARY.mark_pareto_frontier([row])
        self.assertEqual(row["pareto"], "ineligible")
        self.assertIn("missing", row["pareto_reason"])
        self.assertIn("incremental_p50_ms", row["pareto_reason"])

    def test_frontier_crossover_pairs_nearest_fallback_and_exact_caps(self) -> None:
        reports = []
        for cap, contract, elapsed, work, peak in (
            (16, "configured_cap_fallback", 100, 20, 80),
            (32, "exact_frontier", 200, 120, 90),
            (64, "exact_frontier", 210, 130, 95),
        ):
            case = {
                "scenario": "go_inbound_frontier",
                "passed": True,
                "canonical_graph": {"equal": True},
                "frontier_coverage_gate": {"contract": contract, "passed": True},
                "incremental": {
                    "elapsed_ms": elapsed,
                    "indexed_work_elapsed_ms": work,
                    "peak_rss_mb": peak,
                },
                "fresh_fast_full_after_change": {"elapsed_ms": 400},
            }
            item = report(case)
            item["parameters"]["frontier_files"] = 16
            item["parameters"]["config_overrides"][
                "incremental_exact_max_affected_paths"
            ] = str(cap)
            reports.append((f"cap-{cap}", item))

        rows = [SUMMARY.summarize_group(label, [item]) for label, item in reports]
        crossovers = SUMMARY.frontier_crossover_rows(rows)

        self.assertEqual(len(crossovers), 1)
        self.assertEqual(crossovers[0]["fallback_cap"], 16)
        self.assertEqual(crossovers[0]["exact_cap"], 32)
        self.assertEqual(crossovers[0]["affected_files"], 17)
        self.assertEqual(crossovers[0]["exact_fallback_ratio"], 2.0)
        self.assertEqual(crossovers[0]["conclusion"], "fallback faster")
        markdown = SUMMARY.render_markdown(rows)
        self.assertIn("## Exact-frontier cap crossover", markdown)
        self.assertIn("| go_inbound_frontier | 17 | 16 | 100.0 | 20.0 | 80.0 | 32 | 200.0", markdown)
        self.assertIn("2.00×", markdown)
        self.assertIn("fallback faster", markdown)

    def test_markdown_breaks_out_source_mutation_and_reindex_phases(self) -> None:
        case = {
            "scenario": "route_handler",
            "mutation": {
                "description": "HTTP handler source edit with route literal oracle",
                "changed_paths": ["src/ui/http_server.c"],
            },
            "passed": True,
            "canonical_graph": {"equal": True},
            "incremental": {
                "elapsed_ms": 120,
                "indexed_work_elapsed_ms": 45,
                "publish_kind": "incremental_exact",
                "exact_reason": None,
            },
            "fresh_fast_full_after_change": {"elapsed_ms": 600},
            "speedup_full_rebuild_over_incremental": 5.0,
        }
        markdown = SUMMARY.render_markdown([SUMMARY.summarize_group("latest", [report(case)])])

        self.assertIn("## Incremental mutation and reindex breakdown", markdown)
        self.assertIn("HTTP handler source edit with route literal oracle", markdown)
        self.assertIn("src/ui/http_server.c", markdown)
        self.assertIn("incremental_exact", markdown)
        self.assertIn("| 120.0 | 45.0 | 600.0 | 5.00 |", markdown)
        self.assertIn("end-to-end", markdown)
        self.assertIn("isolates indexing work", markdown)

    def test_markdown_computes_latest_speedups_for_matching_capabilities(self) -> None:
        def measured_case(incremental_ms: int, full_ms: int, query_ms: int) -> dict:
            return {
                "passed": True,
                "canonical_graph": {"equal": True},
                "oracles": {
                    "quality": {"passed": True, "passed_count": 1, "applicable_count": 1},
                    "probe": {
                        "elapsed_ms": query_ms,
                        "response_token_estimate": 10,
                    },
                },
                "incremental": {"elapsed_ms": incremental_ms, "peak_rss_mb": 100},
                "fresh_fast_full_after_change": {"elapsed_ms": full_ms},
            }

        rows = [
            SUMMARY.summarize_group("baseline-rank-off", [report(measured_case(20, 100, 8))]),
            SUMMARY.summarize_group("latest-rank-off", [report(measured_case(10, 50, 4))]),
        ]
        markdown = SUMMARY.render_markdown(rows)

        self.assertIn("## Historical performance deltas", markdown)
        self.assertIn("| latest-rank-off | baseline-rank-off | 2.00× | 2.00× | 2.00× |", markdown)

    def test_failed_quality_is_not_pareto_eligible(self) -> None:
        case = {
            "passed": False,
            "canonical_graph": {"equal": True},
            "oracles": {
                "quality": {"passed": False, "passed_count": 1, "applicable_count": 2},
                "marker": {
                    "elapsed_ms": 1,
                    "response_bytes": 4,
                    "response_token_estimate": 1,
                },
            },
            "incremental": {"elapsed_ms": 1, "peak_rss_mb": 1},
            "fresh_fast_full_after_change": {"elapsed_ms": 2, "peak_rss_mb": 2},
        }
        row = SUMMARY.summarize_group("bad-quality", [report(case)])
        SUMMARY.mark_pareto_frontier([row])
        self.assertEqual(row["decision"], "REJECT: task correctness")
        self.assertEqual(row["pareto"], "ineligible")

    def test_atomic_report_write_replaces_content_without_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "summary.md"
            SUMMARY.atomic_write_text(output, "first\n")
            SUMMARY.atomic_write_text(output, "second\n")
            self.assertEqual(output.read_text(), "second\n")
            self.assertEqual(list(output.parent.glob(".summary.md.*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
