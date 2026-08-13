"""Contracts for the rank-evidence registry and bounded suite orchestration."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HYPOTHESES = load_module("rank_hypotheses", "benchmarks/rank_hypotheses.py")
EXPERIMENTS = load_module("rank_experiments_contract", "benchmarks/run_experiments.py")
SUITE = load_module("rank_evidence_suite", "benchmarks/run_evidence_suite.py")
TEST_BUILD = {
    "target": "make -f Makefile.cbm cbm",
    "compiler": "Apple clang version 21.0.0",
    "cflags": "-O3 -DNDEBUG",
}


class RankHypothesisRegistryTest(unittest.TestCase):
    def test_every_hypothesis_has_a_plain_english_name_and_a_consolidated_family(
        self,
    ) -> None:
        self.assertEqual(
            set(HYPOTHESES.HYPOTHESES), {f"H{index}" for index in range(1, 8)}
        )
        for hypothesis_id, hypothesis in HYPOTHESES.HYPOTHESES.items():
            self.assertGreaterEqual(len(hypothesis["name"].split()), 3, hypothesis_id)
            self.assertGreaterEqual(
                len(hypothesis["question"].split()), 8, hypothesis_id
            )
            self.assertIn(hypothesis["family"], HYPOTHESES.HYPOTHESIS_FAMILIES)
            self.assertIn(
                hypothesis_id,
                HYPOTHESES.HYPOTHESIS_FAMILIES[hypothesis["family"]]["hypotheses"],
            )

        # H1 and H5 are two directions over the same utility evidence. Treating them as
        # independent experiments double-counts one observation.
        self.assertEqual(
            HYPOTHESES.HYPOTHESES["H1"]["family"],
            HYPOTHESES.HYPOTHESES["H5"]["family"],
        )
        self.assertEqual(
            HYPOTHESES.HYPOTHESES["H3"]["family"],
            HYPOTHESES.HYPOTHESES["H7"]["family"],
        )

    def test_rank_options_are_exhaustively_classified_by_experimental_role(
        self,
    ) -> None:
        all_options = set().union(*HYPOTHESES.RANK_OPTION_GROUPS.values())
        self.assertEqual(
            len(all_options), sum(map(len, HYPOTHESES.RANK_OPTION_GROUPS.values()))
        )
        self.assertEqual(
            HYPOTHESES.RANK_OPTION_GROUPS["numerical_convergence_controls"],
            {"pagerank_epsilon", "pagerank_max_iter"},
        )
        self.assertEqual(
            HYPOTHESES.RANK_OPTION_GROUPS["rank_lifecycle_policies"],
            {"rank_enabled", "rank_refresh", "rank_scope"},
        )
        self.assertIn(
            "pagerank_damping", HYPOTHESES.RANK_OPTION_GROUPS["ranking_semantics"]
        )
        self.assertTrue(
            HYPOTHESES.EDGE_WEIGHT_OPTIONS
            <= HYPOTHESES.RANK_OPTION_GROUPS["ranking_semantics"]
        )

    def test_scorer_registry_covers_in_out_and_total_without_erasing_legacy_names(
        self,
    ) -> None:
        expected = {
            "degree_unweighted_in",
            "degree_unweighted_out",
            "degree_unweighted_total",
            "degree_weighted_in",
            "degree_weighted_out",
            "degree_weighted_total",
            "calls_in",
            "calls_out",
            "calls_total",
            "pagerank",
            "linkrank_in",
            "importance_pr879",
        }
        self.assertTrue(expected <= set(HYPOTHESES.SCORER_SPECS))
        self.assertEqual(HYPOTHESES.SCORER_ALIASES["degree"], "degree_unweighted_total")
        self.assertEqual(HYPOTHESES.SCORER_ALIASES["in_degree"], "degree_unweighted_in")
        self.assertEqual(HYPOTHESES.SCORER_ALIASES["weighted_in"], "degree_weighted_in")
        self.assertEqual(HYPOTHESES.SCORER_ALIASES["importance"], "importance_pr879")
        for name, scorer in HYPOTHESES.SCORER_SPECS.items():
            self.assertGreaterEqual(len(scorer["label"].split()), 2, name)
            self.assertIn(scorer["direction"], {"in", "out", "total", "global"})


class EvidenceSuiteContractTest(unittest.TestCase):
    def test_full_performance_contract_remains_thirty_nine_cells(self) -> None:
        # This is deliberately computed from the production preset shape, not copied from
        # documentation. Three legacy candidates each get their native profile; latest gets
        # the ten controlled profiles; the 13 cells are repeated three times.
        self.assertEqual(SUITE.automatic_performance_cell_count("full"), 39)
        self.assertEqual(SUITE.automatic_performance_cell_count("quick"), 4)

    def test_suite_names_explain_each_component_and_the_unified_assessment(
        self,
    ) -> None:
        self.assertEqual(
            SUITE.PERFORMANCE_SUITE_NAME,
            "performance-v1 — Incremental indexing cost, correctness, and capability attribution",
        )
        self.assertEqual(
            SUITE.RANK_SUITE_NAME,
            "rank-evidence-v2 — Node/edge ranking quality and runtime-configuration evidence",
        )
        self.assertEqual(
            SUITE.UNIFIED_SUITE_NAME,
            "codebase-memory-evidence-v1 — Unified performance and ranking assessment",
        )

    def test_every_performance_execution_has_a_cell_name_and_plain_english_questions(
        self,
    ) -> None:
        cells = SUITE.performance_cells("full")
        self.assertEqual(len(cells), 39)
        for cell in cells:
            self.assertTrue(cell["cell_name"])
            self.assertGreaterEqual(len(cell["questions"]), 1)
            for question in cell["questions"]:
                self.assertRegex(question["id"], r"^PERF-H\d+$")
                self.assertGreaterEqual(len(question["name"].split()), 3)
                self.assertGreaterEqual(len(question["question"].split()), 8)

    def test_quick_suite_records_runtime_without_a_suite_target_or_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            receipt = SUITE.build_receipt(
                repository=ROOT,
                output_root=Path(tmpdir),
                performance_preset="quick",
                hypotheses_preset="quick",
                rank_build=TEST_BUILD,
            )
        self.assertEqual(receipt["performance"]["expected_cells"], 4)
        self.assertGreater(receipt["hypotheses"]["candidate_cells"], 0)
        self.assertIn("run_experiments.py", " ".join(receipt["performance"]["command"]))
        self.assertIn(
            "campaign_specs.py",
            " ".join(receipt["hypotheses"]["preparation_command"]),
        )
        self.assertIn(
            "--quick-hypotheses", receipt["hypotheses"]["preparation_command"]
        )
        self.assertIn("run_experiments.py", " ".join(receipt["hypotheses"]["command"]))
        self.assertIn("--matrix-spec", receipt["hypotheses"]["command"])
        self.assertNotIn("autotune.py", " ".join(receipt["hypotheses"]["command"]))
        self.assertIn("recovery_command", receipt["performance"])
        self.assertIn("recovery_command", receipt["hypotheses"])
        self.assertIn("git", receipt["environment"])
        self.assertIn("python", receipt["environment"])
        self.assertEqual(
            receipt["hypotheses"]["build"], dict(sorted(TEST_BUILD.items()))
        )
        self.assertEqual(
            receipt["hypotheses"]["build_provenance"]["status"],
            "declared_expectation_pending_artifact_verification",
        )
        self.assertNotIn("deadline", json.dumps(receipt).lower())
        self.assertNotIn("target_seconds", json.dumps(receipt).lower())
        self.assertIn(
            "no suite wall-clock target", receipt["hypotheses"]["runtime_policy"]
        )

    def test_container_receipt_changes_both_canonical_commands_by_the_same_flags(
        self,
    ) -> None:
        receipt = SUITE.build_receipt(
            repository=ROOT,
            output_root=ROOT / ".worktrees" / "evidence-suite-container-test",
            performance_preset="quick",
            hypotheses_preset="quick",
            rank_build=TEST_BUILD,
            container={"cpus": 4.0, "memory": "6g", "workers": 4},
        )

        expected_suffix = [
            "--container",
            "--cpus",
            "4",
            "--memory",
            "6g",
            "--workers",
            "4",
        ]
        self.assertEqual(receipt["execution"]["mode"], "container")
        self.assertEqual(receipt["performance"]["command"][-7:], expected_suffix)
        self.assertEqual(receipt["hypotheses"]["command"][-7:], expected_suffix)
        self.assertNotIn("--container", receipt["hypotheses"]["preparation_command"])
        self.assertEqual(
            receipt["performance"]["public_entrypoint"],
            "benchmarks/run_experiments.py",
        )
        self.assertEqual(
            receipt["hypotheses"]["public_entrypoint"],
            "benchmarks/run_experiments.py",
        )

    def test_full_performance_contract_has_no_suite_duration_control(self) -> None:
        receipt = SUITE.build_receipt(
            repository=ROOT,
            output_root=ROOT / ".worktrees" / "evidence-suite-test",
            performance_preset="full",
            hypotheses_preset="quick",
            rank_build=TEST_BUILD,
        )
        self.assertEqual(receipt["performance"]["expected_cells"], 39)
        self.assertEqual(receipt["performance"]["runtime_policy"], "run_to_completion")
        self.assertNotIn("suite_duration_control", receipt["performance"])


if __name__ == "__main__":
    unittest.main()
