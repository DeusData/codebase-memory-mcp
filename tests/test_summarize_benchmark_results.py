import importlib.util
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
        "parameters": {"config_overrides": {"rank_enabled": "false"}},
        "cleanup": {"requested": True, "removed": True},
        "cases": [case],
    }


class SummarizeBenchmarkResultsTest(unittest.TestCase):
    def test_quality_failure_blocks_acceptance_even_with_high_speedup(self) -> None:
        case = {
            "passed": False,
            "canonical_graph": {"equal": False},
            "oracles": {"passed": True},
            "incremental": {"elapsed_ms": 10, "peak_rss_mb": 80},
            "fresh_fast_full_after_change": {"elapsed_ms": 100, "peak_rss_mb": 90},
            "speedup_full_rebuild_over_incremental": 10.0,
        }
        row = SUMMARY.summarize_group("latest-rank-off", [report(case)])
        self.assertEqual(row["decision"], "REJECT: quality/correctness")
        self.assertEqual(row["canonical"], "0/1")
        self.assertEqual(row["speedup_p50"], 10.0)

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
        self.assertEqual(row["cleanup"], "3/3")
        self.assertEqual(row["decision"], "PASS")

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

    def test_query_quality_size_latency_and_pareto_frontier(self) -> None:
        compact_case = {
            "passed": True,
            "canonical_graph": {"equal": True},
            "oracles": {
                "quality": {"passed": True, "passed_count": 2, "applicable_count": 2},
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
                "quality": {"passed": True, "passed_count": 2, "applicable_count": 2},
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
        self.assertEqual(rows[0]["quality_score"], 1.0)
        self.assertEqual(rows[0]["query_response_p50_bytes"], 80.0)
        self.assertEqual(rows[0]["query_response_p50_tokens"], 20.0)
        self.assertEqual(rows[0]["query_latency_p50_ms"], 5.0)
        self.assertEqual(rows[0]["pareto"], "frontier")
        self.assertEqual(rows[1]["pareto"], "dominated by compact")

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
        self.assertEqual(row["decision"], "REJECT: quality/correctness")
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
