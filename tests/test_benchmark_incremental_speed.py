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
                {"publish_kind": "full", "logfile": str(logfile)},
                "level=info msg=index.supervisor.reap outcome=clean",
                stdout_bytes=10,
                elapsed_ms=100.0,
                include_logs=False,
            )

        self.assertEqual(result["peak_rss_mb"], 320)
        self.assertEqual(result["logged_elapsed_ms"]["pipeline_done"], 81)
        self.assertEqual(len(result["measurement_log_markers"]), 2)
        self.assertNotIn("ignored detail", "\n".join(result["measurement_log_markers"]))

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
