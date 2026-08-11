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

from benchmarks.allocator import large_object_pool_qualification as contract

COMMIT = "a" * 40
PLUGIN_SHA = "b" * 64
DEVICE = "mlx5_0"
NUMA_NODE = 0
ITERATIONS = 1000


def runfile(relative: str) -> Path:
    return Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / relative


def valid_policy() -> dict[str, Any]:
    return json.loads(
        runfile("benchmarks/allocator/large_object_pool_qualification_sla.json").read_text(
            encoding="utf-8"
        )
    )


def valid_report() -> dict[str, Any]:
    policy = valid_policy()
    rows = []
    for mode in policy["required_modes"]:
        for size in policy["required_sizes"]:
            for lifetime in policy["required_lifetimes"]:
                rows.append(
                    {
                        "mode": mode,
                        "bytes": size,
                        "lifetime": lifetime,
                        "registration_lifetime": (
                            "lease"
                            if mode == "device-registration" and lifetime == "batch"
                            else "allocation"
                        ),
                        "operations": ITERATIONS,
                        "elapsed_ns": 1_000_000,
                        "operations_per_second": 1_000_000.0,
                        "p99_ns": 1000,
                        "failures": 0,
                        "internal_fragmentation_bytes": 0,
                        "external_fragmentation_bytes": 0,
                        "hugepage_fallback_allocations": 0,
                        "registration_failures": 0,
                        "deregister_errors": 0,
                        "coalesce_errors": 0,
                        "quota_errors": 0,
                        "numa": {
                            "local_allocations": ITERATIONS,
                            "remote_allocations": 0,
                            "fallback_allocations": 0,
                            "bind_errors": 0,
                        },
                    }
                )
    return {
        "schema": contract.BENCHMARK_SCHEMA,
        "status": "PASSED",
        "qualification_eligible": True,
        "attestation": contract.ATTESTATION,
        "config": {"iterations": ITERATIONS, "pool_bytes": contract.POOL_BYTES},
        "provider": {
            "class": "device",
            "name": "physical-rdma-provider",
            "provenance": "approved-device-provider-v1",
            "device": DEVICE,
            "register_calls": 12000,
            "register_errors": 0,
            "deregister_calls": 12000,
            "deregister_errors": 0,
        },
        "hugepages": {
            "requested": True,
            "actual": True,
            "actual_page_size": 2 * 1024 * 1024,
            "fallback_reason": "none",
            "fallback_errno": 0,
        },
        "locked_memory": {"succeeded": True, "bytes": contract.POOL_BYTES},
        "numa": {
            "linux_native": True,
            "numa_available": True,
            "configured_node": NUMA_NODE,
            "allowed_nodes": [NUMA_NODE, 1],
            "allowed_cpus": [0, 1],
        },
        "contract": {"deregister_errors": 0, "coalesce_errors": 0, "quota_errors": 0},
        "rows": rows,
    }


class LargeObjectPoolQualificationContractTest(unittest.TestCase):
    def validate(self, report: dict[str, Any]) -> list[str]:
        _, errors = contract.validate_report(
            report,
            valid_policy(),
            expected_device=DEVICE,
            expected_numa_node=NUMA_NODE,
            expected_iterations=ITERATIONS,
        )
        return errors

    def test_complete_physical_matrix_passes_contract(self):
        checks, errors = contract.validate_report(
            valid_report(),
            valid_policy(),
            expected_device=DEVICE,
            expected_numa_node=NUMA_NODE,
            expected_iterations=ITERATIONS,
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(checks), 18 * 10)
        self.assertTrue(all(check["passed"] for check in checks))

    def test_mock_missing_device_or_skipped_result_is_nonqualified(self):
        report = valid_report()
        report["status"] = "SKIPPED"
        report["qualification_eligible"] = False
        report["provider"]["class"] = "mock"
        errors = self.validate(report)
        self.assertTrue(any("not PASSED" in error for error in errors))
        self.assertTrue(any("real device provider" in error for error in errors))

    def test_hugepage_actual_and_fallback_tampering_are_rejected(self):
        report = valid_report()
        report["hugepages"]["actual"] = False
        report["hugepages"]["fallback_reason"] = "insufficient-hugepages"
        report["rows"][6]["hugepage_fallback_allocations"] = 1
        errors = self.validate(report)
        self.assertTrue(any("not actual" in error for error in errors))
        self.assertTrue(any("reports fallback" in error for error in errors))
        self.assertTrue(any("hugepage_fallback_allocations" in error for error in errors))

    def test_matrix_downgrade_and_numa_provenance_tampering_are_rejected(self):
        report = valid_report()
        report["rows"].pop()
        report["numa"]["configured_node"] = 7
        errors = self.validate(report)
        self.assertTrue(any("matrix is incomplete" in error for error in errors))
        self.assertTrue(any("NUMA provenance" in error for error in errors))

    def test_independent_throughput_p99_fragmentation_and_cleanup_slas_fail_closed(self):
        report = valid_report()
        row = report["rows"][0]
        row["operations_per_second"] = 0
        row["p99_ns"] = 100_000_001
        row["external_fragmentation_bytes"] = 1
        row["deregister_errors"] = 1
        row["coalesce_errors"] = 1
        row["quota_errors"] = 1
        errors = self.validate(report)
        for metric in (
            "operations_per_second",
            "p99_ns",
            "external_fragmentation_bytes",
            "deregister_errors",
            "coalesce_errors",
            "quota_errors",
        ):
            self.assertTrue(any(metric in error for error in errors), errors)

    def test_dirty_check_includes_untracked_files(self):
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
            source, errors = contract.git_source(repo, commit)
            self.assertEqual(source["state"], "dirty")
            self.assertTrue(any("untracked" in error for error in errors))

    def test_provider_approval_hash_is_checked_without_loading_plugin(self):
        with tempfile.TemporaryDirectory() as temporary:
            plugin = Path(temporary) / "provider.so"
            plugin.write_bytes(b"not an approved plugin")
            evidence, errors = contract.provider_evidence(plugin, PLUGIN_SHA)
            self.assertFalse(evidence["approved"])
            self.assertTrue(any("approved SHA-256" in error for error in errors))

    def test_absent_physical_device_and_link_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence, errors = contract.device_evidence(
                DEVICE, "1", NUMA_NODE, Path(temporary)
            )
            self.assertFalse(evidence["sysfs_backed"])
            self.assertTrue(any("device is absent" in error for error in errors))
            self.assertTrue(any("not ACTIVE" in error for error in errors))
            self.assertTrue(any("not LinkUp" in error for error in errors))

    def test_hashed_artifact_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            records = []
            names = (
                "preflight.json",
                "sla_policy.json",
                "artifact.schema.json",
                "benchmark.json",
                "benchmark.log",
            )
            for name in names:
                path = root / name
                path.write_text("{}\n", encoding="utf-8")
                records.append(contract.artifact(path, root))
            by_path = {record["path"]: record for record in records}
            policy_sha = by_path["sla_policy.json"]["sha256"]
            schema_sha = by_path["artifact.schema.json"]["sha256"]
            manifest = {
                "schema": contract.MANIFEST_SCHEMA,
                "qualification_id": contract.QUALIFICATION_ID,
                "source": {"expected_commit": COMMIT, "commit": COMMIT, "state": "clean"},
                "attestation": contract.ATTESTATION,
                "inputs": {
                    "plugin_sha256": PLUGIN_SHA,
                    "approved_plugin_sha256": PLUGIN_SHA,
                    "policy_sha256": policy_sha,
                    "schema_sha256": schema_sha,
                },
                "preflight": {
                    "provider": {
                        "sha256": PLUGIN_SHA,
                        "approved_sha256": PLUGIN_SHA,
                    }
                },
                "sla": {"policy_sha256": policy_sha},
                "artifacts_complete": True,
                "outcome": "passed",
                "qualification_eligible": True,
                "errors": [],
                "artifacts": records,
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            contract.verify_manifest(manifest_path, COMMIT)
            (root / "benchmark.json").write_text('{"tampered":true}\n', encoding="utf-8")
            with self.assertRaisesRegex(contract.QualificationError, "SHA-256 mismatch"):
                contract.verify_manifest(manifest_path, COMMIT)

    def test_schema_and_policy_are_version_bound(self):
        schema = json.loads(
            runfile(
                "docs/validation/Large_object_pool_qualification_artifact.schema.json"
            ).read_text(encoding="utf-8")
        )
        policy = valid_policy()
        self.assertEqual(
            schema["properties"]["schema"]["const"], contract.MANIFEST_SCHEMA
        )
        self.assertEqual(policy["schema"], contract.SLA_SCHEMA)
        self.assertEqual(set(policy["thresholds"]), contract.REQUIRED_THRESHOLD_KEYS)
        self.assertEqual(policy["required_modes"][2], "device-registration")


if __name__ == "__main__":
    unittest.main()
