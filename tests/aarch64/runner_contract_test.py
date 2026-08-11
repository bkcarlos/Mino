# Copyright 2026 The Mino Authors
# SPDX-License-Identifier: LGPL-3.0-only

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


def load_runner():
    source = (
        Path(os.environ["TEST_SRCDIR"])
        / os.environ["TEST_WORKSPACE"]
        / "tools"
        / "ci"
        / "aarch64_validation.py"
    )
    spec = importlib.util.spec_from_file_location("aarch64_validation", source)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


runner = load_runner()
SHA = "a" * 40


class AArch64RunnerContractTest(unittest.TestCase):
    def valid_preflight(self, **overrides):
        values = {
            "system": "Linux",
            "machine": "aarch64",
            "commit": SHA,
            "expected_commit": SHA,
            "status": "",
            "governors": {"cpu0": "performance", "cpu1": "performance"},
            "expected_governor": "performance",
            "qemu": [],
            "native_attestation": "physical-aarch64",
        }
        values.update(overrides)
        return runner.evaluate_preflight(**values)

    def test_valid_native_contract_passes(self):
        self.assertEqual(self.valid_preflight(), [])

    def test_wrong_architecture_fails_closed(self):
        errors = self.valid_preflight(machine="x86_64")
        self.assertTrue(any("uname -m=aarch64" in error for error in errors))

    def test_dirty_or_commit_mismatch_fails_closed(self):
        errors = self.valid_preflight(
            status="?? evidence/", expected_commit="b" * 40
        )
        self.assertTrue(any("clean" in error for error in errors))
        self.assertTrue(any("commit mismatch" in error for error in errors))

    def test_qemu_and_missing_attestation_fail_closed(self):
        errors = self.valid_preflight(
            qemu=["systemd-detect-virt reports qemu"], native_attestation=""
        )
        self.assertTrue(any("QEMU" in error for error in errors))
        self.assertTrue(any("attestation" in error for error in errors))

    def test_governor_unavailable_or_mixed_fails_closed(self):
        self.assertTrue(any("unavailable" in error for error in self.valid_preflight(governors={})))
        errors = self.valid_preflight(
            governors={"cpu0": "performance", "cpu1": "schedutil"}
        )
        self.assertTrue(any("cpu1" in error for error in errors))

    def test_artifact_requires_nonempty_content_and_hashes_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = root / "result.json"
            value.write_text('{"passed":true}\n', encoding="utf-8")
            record = runner.artifact(value, root)
            self.assertEqual(record["path"], "result.json")
            self.assertEqual(len(record["sha256"]), 64)
            empty = root / "empty.log"
            empty.touch()
            with self.assertRaises(runner.QualificationError):
                runner.artifact(empty, root)

    def test_qualification_rejects_gtest_skip(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "test.log"
            path.write_text(
                "[ RUN      ] AtomicAbiSharedMemoryTest.CrossProcess\n"
                "[  SKIPPED ] AtomicAbiSharedMemoryTest.CrossProcess\n",
                encoding="utf-8",
            )
            with self.assertRaises(runner.QualificationError):
                runner.assert_test_log_has_no_skips(path)

    def test_telemetry_parser_rejects_incomplete_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "telemetry.log"
            path.write_text("baseline 1.000 0.00% 0 0 0\n", encoding="utf-8")
            with self.assertRaises(runner.QualificationError):
                runner.parse_telemetry(path)

    def test_sla_any_failed_check_fails_overall(self):
        policy = {
            "aarch64": {
                "allocator": {"max_failures": 0, "min_cursor_cache_vs_legacy_ops_ratio": 0.9},
                "channel": {"required_artifact_status": "MEASURED", "max_reported_errors": 0, "max_64_subscriber_fanout_p99_ns": 10},
                "bridge": {"required_outcome": "passed", "min_messages_per_second": 1, "max_p99_rtt_us": 10},
                "storage_1k_per_batch": {"max_reported_errors": 0, "min_writer_mebibytes_per_second": 1, "max_fdatasync_p99_ns": 10, "min_recovery_mebibytes_per_second": 1, "min_buffer_records_per_second": 1},
                "telemetry": {"max_dropped": 0, "max_counters_overhead_percent": 10, "max_sampled_1pct_overhead_percent": 10},
            },
            "x86_reference": {
                "storage_1k_per_batch": {
                    "max_encode_p99_ns": 10,
                    "min_writer_mebibytes_per_second": 1,
                    "max_fdatasync_p99_ns": 10,
                    "min_recovery_mebibytes_per_second": 1,
                    "min_buffer_records_per_second": 1,
                }
            },
        }
        measurements = {
            "allocator": {"failures": 0, "cursor_cache_vs_legacy_ops_ratio": 1.0},
            "channel": {"artifact_status": "MEASURED", "reported_errors": 0, "subscribers_64_fanout_p99_ns": 1},
            "bridge": {"outcome": "passed", "minimum_messages_per_second": 2, "maximum_p99_rtt_us": 1},
            "storage": {"reported_errors": 0, "encode_p99_ns": 1, "writer_mebibytes_per_second": 2, "fdatasync_p99_ns": 1, "recovery_mebibytes_per_second": 2, "buffer_records_per_second": 2},
            "telemetry": {"dropped": 0, "counters_overhead_percent": 11, "sampled_1pct_overhead_percent": 1},
        }
        result = runner.evaluate_sla(measurements, policy)
        self.assertFalse(result["passed"])
        failed = [item["metric"] for item in result["checks"] if not item["passed"]]
        self.assertEqual(failed, ["telemetry.counters_overhead_percent"])


if __name__ == "__main__":
    unittest.main()
