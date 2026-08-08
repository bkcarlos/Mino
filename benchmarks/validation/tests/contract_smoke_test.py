# Copyright 2026 The Mino Authors
#
# Licensed under the GNU Lesser General Public License, Version 3.0.

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


VALIDATION_KEYS = {"V-14", "V-15", "V-16", "V-17", "V-18", "V-27"}


def benchmark_path() -> Path:
    return (
        Path(os.environ["TEST_SRCDIR"])
        / os.environ["TEST_WORKSPACE"]
        / "benchmarks"
        / "validation_benchmark"
    )


def run_benchmark(*arguments: str) -> tuple[subprocess.CompletedProcess[str], dict]:
    completed = subprocess.run(
        [str(benchmark_path()), *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return completed, json.loads(completed.stdout)


class ValidationBenchmarkContractSmokeTest(unittest.TestCase):
    def assert_result_keys(self, artifact: dict) -> None:
        self.assertEqual(set(artifact["results"]), VALIDATION_KEYS)

    def test_memory_suite_measured_and_storage_pending(self) -> None:
        completed, artifact = run_benchmark(
            "--suite=memory", "--iterations=1", "--pin-count=1"
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(artifact["artifact_status"], "MEASURED")
        self.assert_result_keys(artifact)
        self.assertEqual(artifact["results"]["V-14"]["status"], "MEASURED")
        self.assertEqual(artifact["results"]["V-15"]["status"], "MEASURED")
        self.assertEqual(
            artifact["results"]["V-18"]["status"], "MEASURED_AND_MODELED"
        )
        self.assertEqual(artifact["results"]["V-27"]["status"], "MEASURED")
        self.assertEqual(artifact["results"]["V-16"]["status"], "PENDING")
        self.assertEqual(artifact["results"]["V-17"]["status"], "PENDING")

    def test_storage_suite_measured_and_memory_pending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            completed, artifact = run_benchmark(
                "--suite=storage",
                "--storage-records=1",
                "--records-per-writer=1",
                "--payload-bytes=64",
                f"--directory={directory}",
            )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(artifact["artifact_status"], "MEASURED")
        self.assert_result_keys(artifact)
        self.assertEqual(artifact["results"]["V-16"]["status"], "MEASURED")
        self.assertEqual(artifact["results"]["V-17"]["status"], "MEASURED")
        for key in ("V-14", "V-15", "V-18", "V-27"):
            self.assertEqual(artifact["results"][key]["status"], "PENDING")

    def test_invalid_configuration_is_failed_with_pending_results(self) -> None:
        completed, artifact = run_benchmark("--iterations=0")
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(artifact["artifact_status"], "FAILED")
        self.assert_result_keys(artifact)
        for result in artifact["results"].values():
            self.assertEqual(result["status"], "PENDING")


if __name__ == "__main__":
    unittest.main()
