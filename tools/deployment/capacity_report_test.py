#!/usr/bin/env python3
"""Contract tests for complete production Topic capacity reports."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

RUNFILES = Path(os.environ["TEST_SRCDIR"]) / os.environ.get("TEST_WORKSPACE", "mino")
REPORT = RUNFILES / "tools/deployment/capacity_report"
VALIDATOR = RUNFILES / "tools/deployment/mino_deploy"
DEPLOYMENT = RUNFILES / "configs/node.production-edge.toml"
SNAPSHOT = RUNFILES / "configs/capacity/coordinator-topic-snapshot.sample.json"
INVENTORY = RUNFILES / "configs/capacity/production-inventory.sample.json"
BUDGET = RUNFILES / "configs/capacity/node-budget.sample.json"
SCHEMA = RUNFILES / "docs/operations/capacity-report.schema.json"
TEST_COMMIT = "a" * 40


def run(*arguments: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(argument) for argument in arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


class CapacityReportTest(unittest.TestCase):
    def base_command(
        self,
        output: Path,
        *,
        snapshot: Path = SNAPSHOT,
        inventory: Path = INVENTORY,
        budget: Path = BUDGET,
    ) -> list[str]:
        return [
            str(REPORT),
            "report",
            "--repo",
            str(RUNFILES),
            "--deployment-config",
            str(DEPLOYMENT),
            "--deployment-validator",
            str(VALIDATOR),
            "--coordinator-snapshot",
            str(snapshot),
            "--inventory",
            str(inventory),
            "--budget",
            str(budget),
            "--contract-schema",
            str(SCHEMA),
            "--expected-commit",
            TEST_COMMIT,
            "--output",
            str(output),
        ]

    def test_sample_generates_pass_but_never_qualification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "report.json"
            completed = run(*self.base_command(output))
            self.assertEqual(completed.returncode, 0, completed.stderr)
            artifact = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(artifact["outcome"], "PASS")
            self.assertFalse(artifact["qualification_eligible"])
            self.assertTrue(artifact["inventory_coverage"]["exact"])
            self.assertEqual(len(artifact["topics"]), 2)
            self.assertEqual(len(artifact["topics"][0]["partitions"]), 2)
            self.assertEqual(len(artifact["topics"][0]["schemas"]), 2)
            self.assertIsNotNone(artifact["topics"][0]["recorder"])
            verified = run(REPORT, "verify", "--artifact", output)
            self.assertEqual(verified.returncode, 0, verified.stderr)
            rejected = run(REPORT, "verify", "--artifact", output, "--require-qualified", "--expected-commit", TEST_COMMIT)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("not qualification eligible", rejected.stderr)

    def test_snapshot_drift_and_empty_production_inventory_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            drifted = root / "snapshot.json"
            snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
            snapshot["topics"].pop()
            drifted.write_text(json.dumps(snapshot), encoding="utf-8")
            output = root / "drift.json"
            completed = run(*self.base_command(output, snapshot=drifted))
            self.assertNotEqual(completed.returncode, 0)
            artifact = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(artifact["outcome"], "FAIL")
            self.assertTrue(any("missing configured production Topics" in error for error in artifact["errors"]))

            empty = root / "inventory.json"
            inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
            inventory["topics"] = []
            empty.write_text(json.dumps(inventory), encoding="utf-8")
            empty_output = root / "empty.json"
            rejected = run(*self.base_command(empty_output, inventory=empty))
            self.assertNotEqual(rejected.returncode, 0)
            failed = json.loads(empty_output.read_text(encoding="utf-8"))
            self.assertTrue(any("inventory is empty" in error for error in failed["errors"]))

    def test_duplicate_and_unknown_snapshot_topics_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = json.loads(SNAPSHOT.read_text(encoding="utf-8"))

            duplicate_path = root / "duplicate.json"
            duplicate = json.loads(json.dumps(source))
            duplicate["topics"].append(duplicate["topics"][0])
            duplicate_path.write_text(json.dumps(duplicate), encoding="utf-8")
            duplicate_output = root / "duplicate-report.json"
            duplicate_run = run(*self.base_command(duplicate_output, snapshot=duplicate_path))
            self.assertNotEqual(duplicate_run.returncode, 0)
            duplicate_report = json.loads(duplicate_output.read_text(encoding="utf-8"))
            self.assertTrue(any("duplicate Topic" in error for error in duplicate_report["errors"]))

            unknown_path = root / "unknown.json"
            unknown = json.loads(json.dumps(source))
            extra = json.loads(json.dumps(unknown["topics"][0]))
            extra["topic_id"] = 999
            extra["name"] = "unknown/production-topic"
            unknown["topics"].append(extra)
            unknown_path.write_text(json.dumps(unknown), encoding="utf-8")
            unknown_output = root / "unknown-report.json"
            unknown_run = run(*self.base_command(unknown_output, snapshot=unknown_path))
            self.assertNotEqual(unknown_run.returncode, 0)
            unknown_report = json.loads(unknown_output.read_text(encoding="utf-8"))
            self.assertTrue(any("unknown production Topics" in error for error in unknown_report["errors"]))

    def test_insufficient_budget_reports_exact_refusal_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            budget_path = root / "budget.json"
            budget = json.loads(BUDGET.read_text(encoding="utf-8"))
            budget["limits"]["recorder_buffer_bytes"] = 16777216
            budget["emergency"]["recorder_buffer_bytes"] = 8388608
            budget_path.write_text(json.dumps(budget), encoding="utf-8")
            output = root / "report.json"
            completed = run(*self.base_command(output, budget=budget_path))
            self.assertNotEqual(completed.returncode, 0)
            artifact = json.loads(output.read_text(encoding="utf-8"))
            refusals = [item for item in artifact["rejections"] if item["dimension"] == "recorder_buffer_bytes"]
            self.assertEqual(len(refusals), 1)
            self.assertIn("emergency", refusals[0]["reason"])
            self.assertGreater(refusals[0]["requested"], refusals[0]["available"])

    def test_what_if_peak_and_all_deltas_are_accounted_or_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            accepted_output = root / "accepted.json"
            accepted = run(
                *self.base_command(accepted_output),
                "--peak-multiplier",
                "1.25",
                "--what-if-topic",
                "telemetry/events",
                "--disk-pause-seconds",
                "12",
                "--add-publishers",
                "1",
                "--add-subscribers",
                "1",
                "--add-bridge-lanes",
                "1",
                "--add-partitions",
                "1",
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            artifact = json.loads(accepted_output.read_text(encoding="utf-8"))
            telemetry = next(topic for topic in artifact["topics"] if topic["name"] == "telemetry/events")
            self.assertEqual(len(telemetry["partitions"]), 3)
            self.assertEqual(telemetry["recorder"]["disk_pause_seconds"], 12)
            self.assertTrue(artifact["scenario"]["accepted"])

            rejected_output = root / "rejected.json"
            rejected = run(
                *self.base_command(rejected_output),
                "--what-if-topic",
                "telemetry/events",
                "--add-subscribers",
                "100",
            )
            self.assertNotEqual(rejected.returncode, 0)
            failed = json.loads(rejected_output.read_text(encoding="utf-8"))
            self.assertTrue(any(item["kind"] == "topic-policy" and item["dimension"] == "subscribers" for item in failed["rejections"]))

    def test_report_and_input_hash_tampering_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory = root / "inventory.json"
            shutil.copyfile(INVENTORY, inventory)
            output = root / "report.json"
            completed = run(*self.base_command(output, inventory=inventory))
            self.assertEqual(completed.returncode, 0, completed.stderr)

            artifact = json.loads(output.read_text(encoding="utf-8"))
            artifact["resources"]["topics"]["committed"] += 1
            output.write_text(json.dumps(artifact), encoding="utf-8")
            tampered = run(REPORT, "verify", "--artifact", output)
            self.assertNotEqual(tampered.returncode, 0)
            self.assertIn("report SHA-256 mismatch", tampered.stderr)

            completed = run(*self.base_command(output, inventory=inventory))
            self.assertEqual(completed.returncode, 0, completed.stderr)
            inventory.write_text(inventory.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            changed_input = run(REPORT, "verify", "--artifact", output)
            self.assertNotEqual(changed_input.returncode, 0)
            self.assertIn("input hash/size mismatch", changed_input.stderr)


if __name__ == "__main__":
    unittest.main()
