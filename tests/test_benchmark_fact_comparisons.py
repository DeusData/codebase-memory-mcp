import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = (
    Path(__file__).resolve().parents[1] / "benchmarks" / "fact_comparisons.py"
)
SPEC = importlib.util.spec_from_file_location("benchmark_fact_comparisons", SCRIPT)
assert SPEC and SPEC.loader
COMPARISONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPARISONS)


def fact_bundle(
    *,
    run_id: str,
    label: str,
    revision: str,
    capabilities: dict,
    elapsed_ms: float,
    scope: dict | None = None,
    cache: dict | None = None,
    host: dict | None = None,
    terminology_sha256: str = "a" * 64,
    generator_revision: str = "b" * 64,
    binary_path: str = "/build/cbm",
    schema_uri: str = "benchmarks/schema/facts-v2.schema.json",
) -> dict:
    return {
        "$schema": schema_uri,
        "schema_version": 2,
        "terminology_version": "1.0.0",
        "terminology_sha256": terminology_sha256,
        "generator_revision": generator_revision,
        "runs": [
            {
                "run_id": run_id,
                "cell_label": label,
                "mode": "self_dogfood",
                "implementation": {
                    "revision": revision,
                    "binary": {
                        "path": binary_path,
                        "sha256": revision[:1] * 64,
                        "size_bytes": 1234,
                    },
                    "build": {"cflags": "-O2"},
                },
                "capabilities": capabilities,
                "scope": scope or {"repository": "fixture", "changed_files": 1},
                "cache": cache or {"repository_graph": "empty"},
                "host": host or {"machine": "arm64", "platform": "fixture"},
                "harness": {
                    "fact_schema_version": 2,
                    "path": "/repo/benchmarks/incremental_speed.py",
                    "sha256": "c" * 64,
                },
            }
        ],
        "steps": [
            {
                "run_id": run_id,
                "step_id": "incremental_index",
                "occurrence_id": run_id,
                "elapsed_ms": elapsed_ms,
            },
            {
                "run_id": run_id,
                "step_id": "semantic_pairs",
                "occurrence_id": run_id[:23] + "f",
                "elapsed_ms": elapsed_ms / 2,
                "parent_occurrence_id": run_id,
            },
        ],
        "results": [
            {
                "run_id": run_id,
                "result_id": "case.edit.graph_gate",
                "kind": "graph_oracle",
                "status": "passed",
                "value": {
                    "scenario": "edit",
                    "policy": "strict",
                    "declared_stale_views": [],
                },
            }
        ],
        "artifacts": [],
    }


def complete_capabilities(**changes: str) -> dict:
    values = {
        "auto_index_deps": "false",
        "rank_enabled": "true",
        "semantic_edges_enabled": "true",
    }
    values.update(changes)
    return {
        "values": values,
        "completeness": "complete_declared_cell",
        "provenance": "fixture",
    }


class BenchmarkFactComparisonTest(unittest.TestCase):
    def generate(self, bundles: list[dict]) -> dict:
        with tempfile.TemporaryDirectory() as tmpdir:
            sources = []
            for index, bundle in enumerate(bundles):
                path = Path(tmpdir) / f"facts-{index}.json"
                path.write_text(json.dumps(bundle), encoding="utf-8")
                sources.append((path, COMPARISONS.load_fact_bundle(path)))
            return COMPARISONS.generate_comparison_document(
                sources,
                generated_at_utc="2026-07-23T00:00:00+00:00",
                terminology_version="1.0.0",
                terminology_sha256="c" * 64,
            )

    def test_parity_join_emits_ratio_with_source_occurrence_ids(self) -> None:
        document = self.generate(
            [
                fact_bundle(
                    run_id="1" * 24,
                    label="left",
                    revision="a" * 40,
                    capabilities=complete_capabilities(),
                    elapsed_ms=12,
                ),
                fact_bundle(
                    run_id="2" * 24,
                    label="right",
                    revision="b" * 40,
                    capabilities=complete_capabilities(),
                    elapsed_ms=6,
                ),
            ]
        )

        comparison = document["comparisons"][0]
        self.assertEqual(comparison["comparison_kind"], "parity_comparison")
        self.assertTrue(comparison["ratio_allowed"])
        incremental = next(
            row
            for row in comparison["step_comparisons"]
            if row["step_id"] == "incremental_index"
        )
        self.assertEqual(incremental["left_elapsed_divided_by_right_elapsed"], 2.0)
        self.assertEqual(incremental["left_source_occurrence_ids"], ["1" * 24])

    def test_capability_delta_never_emits_cross_implementation_ratio(self) -> None:
        document = self.generate(
            [
                fact_bundle(
                    run_id="3" * 24,
                    label="deps-off",
                    revision="a" * 40,
                    capabilities=complete_capabilities(),
                    elapsed_ms=10,
                ),
                fact_bundle(
                    run_id="4" * 24,
                    label="deps-on",
                    revision="b" * 40,
                    capabilities=complete_capabilities(auto_index_deps="true"),
                    elapsed_ms=30,
                ),
            ]
        )

        comparison = document["comparisons"][0]
        self.assertEqual(comparison["comparison_kind"], "capability_delta_comparison")
        self.assertFalse(comparison["ratio_allowed"])
        self.assertEqual(comparison["step_comparisons"], [])
        self.assertEqual(
            comparison["capability_differences"][0]["capability_id"],
            "auto_index_deps",
        )

    def test_unknown_manifest_rejects_parity_and_lifecycle_uses_outer_step(
        self,
    ) -> None:
        unknown = {"status": "unknown", "reason": "not recorded"}
        partial = {
            "values": {"rank_enabled": "true"},
            "completeness": "partial",
            "effective": unknown,
        }
        document = self.generate(
            [
                fact_bundle(
                    run_id="5" * 24,
                    label="left",
                    revision="a" * 40,
                    capabilities=partial,
                    elapsed_ms=20,
                    cache={"os_page_cache": unknown},
                ),
                fact_bundle(
                    run_id="6" * 24,
                    label="right",
                    revision="b" * 40,
                    capabilities=partial,
                    elapsed_ms=10,
                    cache={"os_page_cache": unknown},
                ),
            ]
        )

        self.assertEqual(document["comparisons"][0]["comparison_kind"], "not_eligible")
        self.assertIn(
            "capability manifest is incomplete or unknown",
            document["comparisons"][0]["limitations"],
        )
        lifecycle_steps = document["lifecycle_rows"][0]["steps"]
        self.assertEqual(
            [row["step_id"] for row in lifecycle_steps], ["incremental_index"]
        )
        self.assertEqual(lifecycle_steps[0]["median_elapsed_ms"], 20)

    def test_paths_and_capability_provenance_do_not_change_parity(self) -> None:
        left_capabilities = complete_capabilities()
        right_capabilities = complete_capabilities()
        right_capabilities["provenance"] = "different measurement path"
        document = self.generate(
            [
                fact_bundle(
                    run_id="7" * 24,
                    label="left",
                    revision="a" * 40,
                    capabilities=left_capabilities,
                    elapsed_ms=12,
                    binary_path="/first/build/cbm",
                ),
                fact_bundle(
                    run_id="8" * 24,
                    label="right",
                    revision="b" * 40,
                    capabilities=right_capabilities,
                    elapsed_ms=6,
                    binary_path="/second/build/cbm",
                ),
            ]
        )

        self.assertEqual(
            document["comparisons"][0]["comparison_kind"], "parity_comparison"
        )

    def test_host_or_benchmark_contract_difference_rejects_pair(self) -> None:
        document = self.generate(
            [
                fact_bundle(
                    run_id="9" * 24,
                    label="left",
                    revision="a" * 40,
                    capabilities=complete_capabilities(),
                    elapsed_ms=12,
                ),
                fact_bundle(
                    run_id="a" * 24,
                    label="right",
                    revision="b" * 40,
                    capabilities=complete_capabilities(),
                    elapsed_ms=6,
                    host={"machine": "x86_64", "platform": "fixture"},
                    terminology_sha256="d" * 64,
                ),
            ]
        )

        comparison = document["comparisons"][0]
        self.assertEqual(comparison["comparison_kind"], "not_eligible")
        self.assertIn("host manifest differs", comparison["limitations"])
        self.assertIn("benchmark contract differs", comparison["limitations"])

    def test_duplicate_run_id_is_rejected(self) -> None:
        bundle = fact_bundle(
            run_id="b" * 24,
            label="duplicate",
            revision="a" * 40,
            capabilities=complete_capabilities(),
            elapsed_ms=12,
        )
        with self.assertRaisesRegex(ValueError, "duplicate fact run_id"):
            self.generate([bundle, bundle])

    def test_old_v2_schema_uri_remains_readable(self) -> None:
        bundle = fact_bundle(
            run_id="c" * 24,
            label="old-v2",
            revision="a" * 40,
            capabilities=complete_capabilities(),
            elapsed_ms=12,
            schema_uri="docs/schema/benchmark-facts-v2.schema.json",
        )

        document = self.generate([bundle])

        self.assertEqual(document["source_bundles"][0]["schema_version"], 2)

    def test_external_source_paths_are_hashed_not_disclosed(self) -> None:
        source = Path("/private/user/retained/facts.json")

        portable = COMPARISONS.portable_source_path(source)

        self.assertRegex(portable, r"^external/[0-9a-f]{12}/facts\.json$")
        self.assertNotIn("/private/user", portable)

    def test_nested_absolute_manifest_paths_are_not_disclosed(self) -> None:
        portable = COMPARISONS.portable_value(
            {
                "scope": {
                    "repo": "/private/user/repository",
                    "policy": "detached_worktree",
                }
            }
        )

        self.assertRegex(
            portable["scope"]["repo"], r"^external/[0-9a-f]{12}/repository$"
        )
        self.assertEqual(portable["scope"]["policy"], "detached_worktree")
        self.assertRegex(
            COMPARISONS.portable_value(r"C:\Users\person\repository"),
            r"^external/[0-9a-f]{12}/repository$",
        )


if __name__ == "__main__":
    unittest.main()
