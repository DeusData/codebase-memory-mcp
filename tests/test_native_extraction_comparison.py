from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "benchmarks" / "run_native_extraction_comparison.py"
SPEC = importlib.util.spec_from_file_location("native_extraction_comparison", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_parse_fixed_benchmark_output_with_darwin_resources() -> None:
    output = """
cs bench: 146 lines, 46 defs, 54 calls, 50 resolved (93%), 45 high-conf (83%), 99 usages, 2 type_refs, 3 rw, 3.48 ms
profile: files=1 parse=0.750ms non_lsp=1.675ms lsp=1.002ms total_extract=2.677ms
             7061504  maximum resident set size
            71065035  instructions retired
            21132482  cycles elapsed
             2179528  peak memory footprint
"""
    metrics = MODULE.parse_sample("cs_lsp_bench", output)
    assert metrics == {
        "lines": 146,
        "definitions": 46,
        "calls": 54,
        "resolved": 50,
        "usages": 99,
        "type_refs": 2,
        "read_write": 3,
        "latency_ms": 3.48,
        "files": 1,
        "parse_ms": 0.75,
        "non_lsp_ms": 1.675,
        "lsp_ms": 1.002,
        "total_extract_ms": 2.677,
        "max_rss_bytes": 7061504,
        "instructions": 71065035,
        "cycles": 21132482,
        "peak_footprint_bytes": 2179528,
    }


def test_parse_shared_parse_baseline_output() -> None:
    output = """
bench: 10 lines, 0 defs, 0 calls, 0 resolved (0%), 0 usages, 0 type_refs, 0 rw, 41.25 ms
profile: files=1024 parse=7.250ms non_lsp=20.125ms lsp=0.000ms total_extract=20.125ms
"""
    metrics = MODULE.parse_sample("shared_parse_baseline", output)
    assert metrics["definitions"] == 0
    assert metrics["calls"] == 0
    assert metrics["resolved"] == 0
    assert metrics["latency_ms"] == 41.25
    assert metrics["files"] == 1024


def test_parse_startup_resource_baseline() -> None:
    output = """
             2097152  maximum resident set size
            26836822  instructions retired
             8401433  cycles elapsed
             1409432  peak memory footprint
"""
    assert MODULE.parse_sample(MODULE.STARTUP_SUITE, output) == {
        "max_rss_bytes": 2097152,
        "instructions": 26836822,
        "cycles": 8401433,
        "peak_footprint_bytes": 1409432,
    }


def test_parse_scale_output_and_linux_rss() -> None:
    output = """
scale: 100=20.8ms (calls=400 resolved=400)  500=129.3ms (calls=2000 resolved=2000)  2000=1210.2ms (calls=8000 resolved=8000)
profile: files=3 parse=32.066ms non_lsp=479.840ms lsp=850.389ms total_extract=1330.229ms
Maximum resident set size (kbytes): 44032
"""
    metrics = MODULE.parse_sample("py_lsp_scale", output)
    assert metrics["small_ms"] == 20.8
    assert metrics["medium_ms"] == 129.3
    assert metrics["latency_ms"] == 1210.2
    assert metrics["calls"] == 8000
    assert metrics["resolved"] == 8000
    assert metrics["max_rss_bytes"] == 44032 * 1024


def test_summarize_reports_per_candidate_suite_medians() -> None:
    samples = [
        {"candidate": candidate, "suite": suite, "metrics": {"latency_ms": value}}
        for candidate, value in (("left", 1.0), ("left", 3.0), ("right", 5.0), ("right", 7.0))
        for suite in MODULE.SUITES
    ]
    medians = MODULE.summarize(samples)
    assert medians["left"]["cs_lsp_bench"]["latency_ms"] == 2.0
    assert medians["right"]["py_lsp_scale"]["latency_ms"] == 6.0


def test_compare_candidates_reports_parity_and_paired_deltas() -> None:
    samples = []
    for suite in MODULE.SUITES:
        for repetition, left_latency, right_latency in ((1, 10.0, 11.0), (2, 20.0, 18.0)):
            samples.extend(
                [
                    {
                        "candidate": "left",
                        "suite": suite,
                        "repetition": repetition,
                        "metrics": {
                            "calls": 4,
                            "resolved": 4,
                            "latency_ms": left_latency,
                            "peak_footprint_bytes": 100,
                        },
                    },
                    {
                        "candidate": "right",
                        "suite": suite,
                        "repetition": repetition,
                        "metrics": {
                            "calls": 4,
                            "resolved": 3,
                            "latency_ms": right_latency,
                            "peak_footprint_bytes": 100,
                        },
                    },
                ]
            )

    comparison = MODULE.compare_candidates(samples)["right_vs_left"]["cs_lsp_bench"]
    assert comparison["paired_samples"] == 2
    assert comparison["output_parity"] is False
    assert comparison["output_differences"]["resolved"] == [
        {"candidate": 3, "baseline": 4}
    ]
    assert comparison["paired_median_percent_change"]["latency_ms"] == 0.0
    assert comparison["paired_median_percent_change"]["peak_footprint_bytes"] == 0.0


def test_add_incremental_memory_metrics_subtracts_matching_startup_run() -> None:
    samples = [
        {
            "candidate": "merged",
            "suite": MODULE.STARTUP_SUITE,
            "repetition": 1,
            "metrics": {"max_rss_bytes": 200, "peak_footprint_bytes": 125},
        },
        {
            "candidate": "merged",
            "suite": "shared_parse_baseline",
            "repetition": 1,
            "metrics": {"max_rss_bytes": 350, "peak_footprint_bytes": 300},
        },
    ]

    MODULE.add_incremental_memory_metrics(samples)

    assert samples[0]["metrics"] == {"max_rss_bytes": 200, "peak_footprint_bytes": 125}
    assert samples[1]["metrics"]["incremental_max_rss_bytes"] == 150
    assert samples[1]["metrics"]["incremental_peak_footprint_bytes"] == 175
