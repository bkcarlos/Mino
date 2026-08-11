# Copyright 2026 The Mino Authors
# SPDX-License-Identifier: LGPL-3.0-only

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from typing import Any

from benchmarks import storage_partition_qualification as qualification

COMMIT = "a" * 40


def runfile(relative: str) -> Path:
    return Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / relative


def benchmark_report(policy: dict[str, Any], target_match: bool = True) -> dict[str, Any]:
    records = policy["workload"]["records_per_scenario"]
    scenarios = []
    baseline = float(records)
    for partitions in qualification.REQUIRED_PARTITIONS:
        partition_results = []
        rates = []
        for partition_id in range(partitions):
            partition_records = records // partitions + (
                1 if partition_id < records % partitions else 0
            )
            elapsed_ns = 1_000_000_000
            rate = float(partition_records)
            rates.append(rate)
            partition_results.append(
                {
                    "partition_id": partition_id,
                    "attempted_records": partition_records,
                    "accepted_records": partition_records,
                    "dequeued_records": partition_records,
                    "written_records": partition_records,
                    "errors": 0,
                    "elapsed_ns": elapsed_ns,
                    "records_per_second": rate,
                }
            )
        throughput = float(records)
        scaling = throughput / baseline
        scenarios.append(
            {
                "partitions": partitions,
                "records": records,
                "attempted_records": records,
                "accepted_records": records,
                "dequeued_records": records,
                "written_records": records,
                "errors": 0,
                "stable_partition_map": {
                    "map_version": 1,
                    "generation": 1,
                    "partition_count": partitions,
                    "strategy": "hash",
                    "state": "active",
                    "hash_algorithm_version": qualification.STABLE_HASH_VERSION,
                    "hash_seed": qualification.STABLE_HASH_SEED,
                },
                "elapsed_ns": 1_000_000_000,
                "records_per_second": throughput,
                "scaling": scaling,
                "scaling_efficiency": scaling / partitions,
                "partition_throughput_imbalance_ratio": max(rates) / min(rates),
                "record_latency_p50_ns": 500,
                "record_latency_p99_ns": 1000,
                "partition_results": partition_results,
            }
        )
    label = policy["target"]["hardware_label"]
    return {
        "schema": qualification.BENCHMARK_SCHEMA,
        "validation": "V-24",
        "configuration": {
            "records": records,
            "payload_bytes": policy["workload"]["payload_bytes"],
            "partition_counts": qualification.REQUIRED_PARTITIONS,
        },
        "errors": 0,
        "qualification": {
            "target": label if target_match else "",
            "required_target": label,
            "eligible": target_match,
        },
        "single_writer_threshold_records_per_second": baseline,
        "target_ingress_records_per_second": policy["workload"][
            "target_ingress_records_per_second"
        ],
        "partitioning_required_for_target": True,
        "scenarios": scenarios,
    }


class StoragePartitionQualificationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy_path = runfile(
            "benchmarks/storage_partition_qualification_sla.json"
        )
        cls.schema_path = runfile(
            "docs/validation/Storage_partition_qualification_artifact.schema.json"
        )
        cls.policy = qualification.load_policy(cls.policy_path)

    def test_complete_partition_matrix_is_accepted_and_sla_is_evaluated(self):
        report = benchmark_report(self.policy)
        self.assertEqual(
            qualification.validate_benchmark_report(report, self.policy, True), []
        )
        sla = qualification.evaluate_sla([report, report], self.policy)
        self.assertTrue(sla["passed"])
        self.assertEqual(len(sla["checks"]), 6)

    def test_hashed_json_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = root / "benchmark.json"
            result.write_text("{}\n", encoding="utf-8")
            record = qualification.artifact(result, root)
            result.write_text('{"tampered":true}\n', encoding="utf-8")
            errors = qualification.verify_artifact(record, root)
            self.assertTrue(any("SHA-256 mismatch" in error for error in errors))

    def test_missing_partition_is_rejected(self):
        report = benchmark_report(self.policy)
        report["scenarios"] = report["scenarios"][:-1]
        errors = qualification.validate_benchmark_report(report, self.policy, True)
        self.assertTrue(any("1/2/4/8/16" in error for error in errors), errors)

    def test_malformed_metrics_and_boolean_errors_fail_closed_without_crashing(self):
        report = benchmark_report(self.policy)
        report["errors"] = False
        report["scenarios"][0]["records_per_second"] = "not-a-number"
        errors = qualification.validate_benchmark_report(report, self.policy, True)
        self.assertTrue(any("integer zero" in error for error in errors), errors)
        self.assertTrue(any("throughput" in error for error in errors), errors)

    def test_claimed_or_fake_hardware_is_rejected_and_real_nontarget_is_nonqualified(self):
        self.assertTrue(
            qualification.validate_attestation(
                "i9-9900KS; Samsung 980 PRO NVMe; ext4"
            )
        )
        target = self.policy["target"]
        host = {
            "system": "Linux",
            "machine": "x86_64",
            "cpu_model": "Intel(R) Core(TM) i9-9900KS CPU @ 4.00GHz",
            "cpu_governors": {"cpu0": target["cpu_governor"]},
        }
        mount = {
            "filesystem": "tmpfs",
            "mount_options": ["rw"],
            "probe_error": "",
        }
        storage = {
            "model": "Samsung SSD 980 PRO 2TB",
            "transport": "nvme",
            "rotational": False,
            "type": "disk",
            "probe_error": "",
        }
        matches, reasons = qualification.evaluate_target(host, mount, storage, target)
        self.assertFalse(matches)
        self.assertTrue(any("filesystem" in reason for reason in reasons))
        self.assertEqual(
            qualification.derive_outcome([], True, matches, True),
            ("nonqualified", False),
        )

    def test_dirty_worktree_including_untracked_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            (repo / "tracked").write_text("tracked\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked"], cwd=repo, check=True)
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Mino Test",
                    "-c",
                    "user.email=test@mino.invalid",
                    "commit",
                    "-qm",
                    "test",
                ],
                cwd=repo,
                check=True,
            )
            commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=repo, text=True
            ).strip()
            (repo / "untracked").write_text("tamper\n", encoding="utf-8")
            source, errors = qualification.git_source(repo, commit)
            self.assertEqual(source["state"], "dirty")
            self.assertTrue(any("dirty worktree" in error for error in errors))

    def test_schema_policy_and_fail_closed_outcomes_are_version_bound(self):
        schema = json.loads(self.schema_path.read_text(encoding="utf-8"))
        self.assertEqual(
            schema["properties"]["schema"]["const"], qualification.ARTIFACT_SCHEMA
        )
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(self.policy["required_partition_counts"], [1, 2, 4, 8, 16])
        self.assertEqual(
            qualification.derive_outcome(["tamper"], True, True, True),
            ("failed", False),
        )
        self.assertEqual(
            qualification.derive_outcome([], True, True, True),
            ("passed", True),
        )


if __name__ == "__main__":
    unittest.main()
