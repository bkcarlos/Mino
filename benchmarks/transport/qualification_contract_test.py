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

from benchmarks.transport import transport_qualification as contract

COMMIT = "a" * 40
RUN_ID = "123456"
RUN_ATTEMPT = "2"
NONCE = "qualification-session-nonce-123456"
PLUGIN_SHA = "b" * 64
BENCHMARK_SHA = "c" * 64
PROVENANCE = "approved-device-provider-v1"


def runfile(relative: str) -> Path:
    return Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / relative


class EvidenceFixture:
    def __init__(self, root: Path, qualification: str):
        self.root = root
        self.qualification = qualification
        self.kinds = [] if qualification == "rdma" else ["ipcf", "ntb", "cxl"]
        self.server_root = root / "server"
        self.client_root = root / "client"
        self.server_root.mkdir(parents=True)
        self.client_root.mkdir(parents=True)
        self.output = root / "final" / "manifest.json"
        self.policy_path = runfile("benchmarks/transport/transport_qualification_sla.json")
        _, self.policy = contract.load_policy(self.policy_path, qualification)
        self.manifests = {
            "server": self._role("server", self.server_root),
            "client": self._role("client", self.client_root),
        }
        self.write_manifests()

    def _row(self, case_id: str, payload_bytes: int) -> dict[str, object]:
        transport, copy_mode = contract.expected_transport_and_copy(case_id)
        return {
            "transport": transport,
            "copy_mode": copy_mode,
            "payload_bytes": payload_bytes,
            "wire_bytes": payload_bytes + 80,
            "iterations": contract.MIN_ITERATIONS,
            "p50_rtt_ns": 100,
            "p99_rtt_ns": 200,
            "elapsed_ns": 1000,
            "process_cpu_ns": 500,
            "payload_bytes_per_second": 1000000,
            "provider_provenance": "builtin-posix" if case_id in ("tcp", "udp") else PROVENANCE,
        }

    def _physical(self) -> dict[str, object]:
        if self.qualification == "rdma":
            return {
                "device": "mlx5_0",
                "sysfs_class": "infiniband",
                "driver": "mlx5_core",
                "ports": [
                    {
                        "port": "1",
                        "state": "4: ACTIVE",
                        "physical_state": "5: LinkUp",
                        "link_layer": "Ethernet",
                        "rate": "100 Gb/sec",
                    }
                ],
            }
        return {
            "device_path": "/sys/devices/pci0000:00/0000:00:01.0",
            "link_state_path": "/sys/devices/pci0000:00/0000:00:01.0/link_state",
            "link_state": "ACTIVE LINKUP",
            "sysfs_subsystem": "pci",
            "sysfs_class": "0x050000",
            "driver": "physical_fabric",
            "validated_kinds": list(self.kinds),
        }

    def _role(self, role: str, role_root: Path) -> dict[str, Any]:
        server_identity = contract.endpoint_identity("10.10.0.1", 45106, 101, 11)
        client_identity = contract.endpoint_identity("10.10.0.2", 45106, 202, 22)
        local = server_identity if role == "server" else client_identity
        peer = client_identity if role == "server" else server_identity
        manifest = contract.role_manifest_base(
            qualification=self.qualification,
            qualification_id="D6-06" if self.qualification == "rdma" else "D6-07",
            role=role,
            run_id=RUN_ID,
            run_attempt=RUN_ATTEMPT,
            session_nonce=NONCE,
            source={"expected_commit": COMMIT, "commit": COMMIT, "state": "clean"},
            local=local,
            peer=peer,
            payloads=contract.REQUIRED_PAYLOADS,
            iterations=contract.MIN_ITERATIONS,
            kinds=self.kinds,
        )
        manifest["identity"]["hostname"] = "physical-a" if role == "server" else "physical-b"
        manifest["benchmark"] = {"name": "transport_matrix_benchmark", "sha256": BENCHMARK_SHA}
        manifest["provider"] = {
            "plugin": f"/opt/mino/{role}/provider.so",
            "plugin_sha256": PLUGIN_SHA,
            "approved_plugin_sha256": PLUGIN_SHA,
            "abi_version": 1,
            "class": "device",
            "class_validation": "production-loader-enforced",
            "provenance": PROVENANCE,
        }
        manifest["physical"] = self._physical()
        manifest["sla"]["policy"] = self.policy
        manifest["sla"]["policy_sha256"] = contract.sha256(self.policy_path)
        artifacts = []
        checks = []
        cases = []
        for case_id in contract.expected_case_ids(self.qualification, self.kinds):
            log = role_root / f"{case_id}.log"
            log.write_text(f"$ benchmark --case={case_id}\n", encoding="utf-8")
            log_record = contract.artifact(log, role_root)
            artifacts.append(log_record)
            rows = []
            result_record = None
            if role == "client":
                result = role_root / f"{case_id}.jsonl"
                result.write_text(
                    "".join(
                        json.dumps(self._row(case_id, payload_bytes)) + "\n"
                        for payload_bytes in contract.REQUIRED_PAYLOADS
                    ),
                    encoding="utf-8",
                )
                result_record = contract.artifact(result, role_root)
                artifacts.append(result_record)
                rows, case_checks, errors = contract.validate_rows(
                    case_id=case_id,
                    rows=contract.parse_jsonl(result),
                    payloads=contract.REQUIRED_PAYLOADS,
                    iterations=contract.MIN_ITERATIONS,
                    provider_provenance_value=PROVENANCE,
                    case_policy=self.policy["cases"][case_id],
                )
                if errors:
                    raise AssertionError(errors)
                checks.extend(case_checks)
            cases.append(
                {
                    "case": case_id,
                    "elapsed_ns": 2000,
                    "log": log_record,
                    "result": result_record,
                    "rows": rows,
                }
            )
        manifest["matrix"]["cases"] = cases
        manifest["artifacts"] = artifacts
        manifest["sla"]["checks"] = checks
        manifest["sla"]["passed"] = True
        manifest["artifacts_complete"] = True
        manifest["qualification_eligible"] = False
        manifest["outcome"] = "passed"
        manifest["errors"] = []
        return manifest

    def write_manifests(self) -> None:
        for role, role_root in (("server", self.server_root), ("client", self.client_root)):
            (role_root / "manifest.json").write_text(
                json.dumps(self.manifests[role], indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

    def finalize(self) -> tuple[int, dict[str, Any]]:
        result = contract.finalize(
            qualification=self.qualification,
            server_root=self.server_root,
            client_root=self.client_root,
            output_path=self.output,
            policy_path=self.policy_path,
            expected_commit=COMMIT,
            expected_run_id=RUN_ID,
            expected_run_attempt=RUN_ATTEMPT,
            expected_kinds=self.kinds,
        )
        return result, json.loads(self.output.read_text(encoding="utf-8"))


class TransportQualificationContractTest(unittest.TestCase):
    def fixture(self, qualification: str = "rdma"):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return EvidenceFixture(Path(temporary.name), qualification)

    def assert_failed(self, fixture: EvidenceFixture, message: str) -> dict[str, Any]:
        fixture.write_manifests()
        code, final = fixture.finalize()
        self.assertEqual(code, 1)
        self.assertEqual(final["outcome"], "failed")
        self.assertFalse(final["qualification_eligible"])
        self.assertTrue(any(message in error for error in final["errors"]), final["errors"])
        return final

    def test_valid_rdma_roles_produce_one_eligible_final_result(self):
        fixture = self.fixture()
        code, final = fixture.finalize()
        self.assertEqual(code, 0)
        self.assertEqual(final["schema"], contract.SCHEMA)
        self.assertTrue(final["artifacts_complete"])
        self.assertTrue(final["qualification_eligible"])
        self.assertEqual(final["results"], [{"kind": "rdma", "outcome": "passed", "sla_passed": True}])

    def test_dirty_check_includes_untracked_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            (repo / "tracked").write_text("tracked\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked"], cwd=repo, check=True)
            subprocess.run(
                ["git", "-c", "user.name=Mino Test", "-c", "user.email=test@mino.invalid", "commit", "-qm", "test"],
                cwd=repo,
                check=True,
            )
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
            (repo / "untracked").write_text("tamper\n", encoding="utf-8")
            source, errors = contract.git_source(repo, commit)
            self.assertEqual(source["state"], "dirty")
            self.assertTrue(any("untracked" in error for error in errors))

    def test_session_and_symmetric_endpoint_tampering_are_rejected(self):
        fixture = self.fixture()
        fixture.manifests["client"]["run"]["session_nonce"] = "different-session-nonce-123456"
        fixture.manifests["client"]["identity"]["peer"]["node_id"] = 999
        final = self.assert_failed(fixture, "session nonce differ")
        self.assertTrue(any("not the client peer" in error for error in final["errors"]))

    def test_unapproved_plugin_is_not_loaded_for_provenance(self):
        with tempfile.TemporaryDirectory() as temporary:
            plugin = Path(temporary) / "provider.so"
            plugin.write_bytes(b"not an approved shared object")
            original = contract.plugin_provenance
            contract.plugin_provenance = lambda *_: (_ for _ in ()).throw(
                AssertionError("unapproved plugin was loaded")
            )
            try:
                evidence, errors = contract.provider_evidence(plugin, "rdma", PLUGIN_SHA)
            finally:
                contract.plugin_provenance = original
            self.assertTrue(any("approved SHA-256" in error for error in errors))
            self.assertEqual(evidence["class"], "unverified")

    def test_benchmark_and_approved_plugin_hash_tampering_are_rejected(self):
        fixture = self.fixture()
        fixture.manifests["client"]["source"]["commit"] = "f" * 40
        fixture.manifests["client"]["benchmark"]["sha256"] = "d" * 64
        fixture.manifests["client"]["provider"]["plugin_sha256"] = "e" * 64
        final = self.assert_failed(fixture, "benchmark evidence differ")
        self.assertTrue(any("clean exact expected commit" in error for error in final["errors"]))
        self.assertTrue(any("approved SHA-256" in error or "plugin hash" in error for error in final["errors"]))

    def test_malformed_nested_manifest_is_archived_as_failed_not_crashed(self):
        fixture = self.fixture()
        fixture.manifests["client"]["matrix"] = None
        self.assert_failed(fixture, "case matrix is incomplete")

    def test_hashed_result_artifact_tampering_is_rejected(self):
        fixture = self.fixture()
        with (fixture.client_root / "rdma.jsonl").open("a", encoding="utf-8") as output:
            output.write("{}\n")
        self.assert_failed(fixture, "artifact size or SHA-256 mismatch")

    def test_bilateral_payload_matrix_downgrade_is_rejected(self):
        fixture = self.fixture()
        fixture.manifests["server"]["matrix"]["payloads"] = [128]
        fixture.manifests["client"]["matrix"]["payloads"] = [128]
        self.assert_failed(fixture, "payload matrix must be exactly")

    def test_metric_type_positive_and_independent_thresholds_fail_closed(self):
        fixture = self.fixture()
        case = fixture.manifests["client"]["matrix"]["cases"][2]
        result_path = fixture.client_root / case["result"]["path"]
        rows = contract.parse_jsonl(result_path)
        rows[0]["p50_rtt_ns"] = "100"
        rows[0]["payload_bytes_per_second"] = 0
        result_path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
        )
        new_record = contract.artifact(result_path, fixture.client_root)
        case["result"] = new_record
        case["rows"] = rows
        for index, record in enumerate(fixture.manifests["client"]["artifacts"]):
            if record["path"] == new_record["path"]:
                fixture.manifests["client"]["artifacts"][index] = new_record
        self.assert_failed(fixture, "must be a positive integer")

    def test_provider_class_and_physical_link_tampering_are_rejected(self):
        fixture = self.fixture()
        fixture.manifests["server"]["provider"]["class"] = "mock"
        fixture.manifests["client"]["physical"]["ports"][0]["state"] = "1: DOWN"
        final = self.assert_failed(fixture, "real device provider ABI/class")
        self.assertTrue(any("device/class/link" in error for error in final["errors"]))

    def test_fabric_requires_separate_ipcf_ntb_and_cxl_results(self):
        fixture = self.fixture("fabric")
        code, final = fixture.finalize()
        self.assertEqual(code, 0)
        self.assertEqual([item["kind"] for item in final["results"]], ["ipcf", "ntb", "cxl"])
        for role in ("server", "client"):
            manifest = fixture.manifests[role]
            manifest["matrix"]["kinds"] = ["ipcf", "ntb"]
            manifest["matrix"]["expected_cases"] = ["tcp", "fabric-ipcf", "fabric-ntb"]
            manifest["matrix"]["cases"] = manifest["matrix"]["cases"][:-1]
            retained = {
                record["path"]
                for case in manifest["matrix"]["cases"]
                for record in (case["log"], case["result"])
                if record is not None
            }
            manifest["artifacts"] = [record for record in manifest["artifacts"] if record["path"] in retained]
            if role == "client":
                manifest["sla"]["checks"] = [
                    check for check in manifest["sla"]["checks"] if check["case"] != "fabric-cxl"
                ]
        self.assert_failed(fixture, "promised kind/case matrix mismatch")

    def test_schema_and_policy_artifacts_are_version_bound(self):
        schema = json.loads(
            runfile("docs/validation/Transport_qualification_artifact.schema.json").read_text(encoding="utf-8")
        )
        policy = json.loads(
            runfile("benchmarks/transport/transport_qualification_sla.json").read_text(encoding="utf-8")
        )
        self.assertEqual(schema["properties"]["schema"]["const"], contract.SCHEMA)
        self.assertEqual(policy["schema"], contract.SLA_SCHEMA)
        self.assertEqual(policy["qualifications"]["fabric"]["required_kinds"], ["ipcf", "ntb", "cxl"])


if __name__ == "__main__":
    unittest.main()
