import importlib.util
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
        header = markdown.splitlines()[2]
        self.assertLess(header.index("Decision"), header.index("Speedup p50"))
        self.assertIn("Binary SHA-256", markdown)


if __name__ == "__main__":
    unittest.main()
