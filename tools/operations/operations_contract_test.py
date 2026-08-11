#!/usr/bin/env python3
"""Contracts preventing D6-17 runbook, alert, and drill scenario drift."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import unittest


def runfile(path: str) -> Path:
    return Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / path


def markdown_anchors(text: str) -> set[str]:
    anchors: set[str] = set()
    for heading in re.findall(r"^#{1,6}\s+(.+?)\s*$", text, re.MULTILINE):
        slug = heading.strip().lower()
        slug = re.sub(r"[^a-z0-9\s-]", "", slug)
        slug = re.sub(r"[\s-]+", "-", slug).strip("-")
        if slug:
            anchors.add(slug)
    return anchors


class OperationsContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest_path = runfile("configs/drills/operations_drills.json")
        runner_path = runfile("tools/operations/drill_runner.py")
        spec = importlib.util.spec_from_file_location("drill_runner", runner_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls.runner = module
        cls.manifest = module.load_and_validate_manifest(cls.manifest_path)
        cls.runbook = runfile("docs/operations/runbook.md").read_text(encoding="utf-8")
        cls.index = runfile("docs/operations/README.md").read_text(encoding="utf-8")
        cls.monitoring = runfile("docs/operations/monitoring.md").read_text(
            encoding="utf-8"
        )
        cls.alerts = json.loads(
            runfile("configs/alerts/mino.rules.yml").read_text(encoding="utf-8")
        )

    def test_every_required_runbook_has_six_phase_contract(self):
        required = {
            "deployment-and-preflight",
            "start-and-stop",
            "certificate-rotation",
            "region-and-acl-denial",
            "bridge-disconnection",
            "subscriber-lease-expiration",
            "schema-incident",
            "storage-disk-failure",
            "capacity-exhaustion",
            "monitoring-and-exporter-failure",
            "rolling-upgrade",
            "backup-and-restore",
            "incident-escalation",
        }
        anchors = markdown_anchors(self.runbook)
        self.assertTrue(required.issubset(anchors), required - anchors)
        sections = re.split(r"(?=^##\s+)", self.runbook, flags=re.MULTILINE)
        by_anchor = {}
        for section in sections:
            heading = re.search(r"^##\s+(.+?)\s*$", section, re.MULTILINE)
            if heading:
                by_anchor.update(
                    {anchor: section for anchor in markdown_anchors("## " + heading.group(1))}
                )
        fields = ("检测指标/告警", "确认命令", "止损", "恢复", "验证", "升级条件")
        for anchor in sorted(required):
            self.assertIn(anchor, by_anchor)
            for field in fields:
                self.assertIn(f"**{field}**", by_anchor[anchor], f"{anchor}: {field}")
            self.assertIn(f"runbook.md#{anchor}", self.index)

    def test_scenarios_alerts_and_runbooks_are_bidirectionally_bound(self):
        alert_names = {
            rule["alert"]
            for group in self.alerts["groups"]
            for rule in group["rules"]
        }
        runbook_anchors = markdown_anchors(self.runbook)
        scenario_ids = set()
        referenced_alerts = set()
        for scenario in self.manifest["scenarios"]:
            scenario_ids.add(scenario["id"])
            self.assertIn(scenario["runbook_anchor"], runbook_anchors)
            self.assertIn(scenario["id"], self.runbook)
            self.assertTrue(set(scenario["alert_ids"]).issubset(alert_names))
            referenced_alerts.update(scenario["alert_ids"])
            self.assertGreaterEqual(
                scenario["modes"]["qualification"]["budget_seconds"],
                scenario["modes"]["quick"]["budget_seconds"],
            )
        self.assertEqual(scenario_ids, set(self.manifest["required_scenarios"]))
        monitoring_anchors = markdown_anchors(self.monitoring)
        for group in self.alerts["groups"]:
            for rule in group["rules"]:
                alert = rule["alert"]
                self.assertIn(alert, self.runbook, f"alert missing from operations map: {alert}")
                runbook_url = rule["annotations"]["runbook_url"]
                self.assertIn("/docs/operations/monitoring.md#", runbook_url)
                anchor = runbook_url.rsplit("#", 1)[1]
                self.assertIn(anchor, monitoring_anchors, f"stale runbook URL: {alert}")
        self.assertTrue(referenced_alerts)

    def test_gtest_filters_still_name_real_checked_in_tests(self):
        sources = "\n".join(
            runfile(path).read_text(encoding="utf-8")
            for path in (
                "mino/security/tls_test.cc",
                "mino/bridge/bridge_pipeline_test.cc",
                "mino/bridge/connection_manager_test.cc",
                "mino/registry/registry_test.cc",
                "mino/runtime/runtime_recovery_stress_test.cc",
                "mino/storage/storage_fault_test.cc",
                "mino/observability/exporter_test.cc",
                "mino/capacity/capacity_test.cc",
                "mino/upgrade/rolling_upgrade_integration_test.cc",
            )
        )
        labels = set()
        for scenario in self.manifest["scenarios"]:
            for mode in ("quick", "qualification"):
                for step in scenario["modes"][mode]["steps"]:
                    for argument in step["argv"]:
                        if argument.startswith("//"):
                            labels.add(argument)
                        marker = "--gtest_filter="
                        if marker not in argument:
                            continue
                        filters = argument.split(marker, 1)[1].split(":")
                        for test_filter in filters:
                            suite, test = test_filter.split(".", 1)
                            pattern = (
                                r"TEST(?:_F|_P)?\(\s*"
                                + re.escape(suite)
                                + r"\s*,\s*"
                                + re.escape(test)
                                + r"\s*\)"
                            )
                            self.assertRegex(sources, pattern, test_filter)
        for label in labels:
            package, target = label[2:].split(":", 1)
            build = runfile(f"{package}/BUILD.bazel").read_text(encoding="utf-8")
            self.assertRegex(build, rf'name\s*=\s*"{re.escape(target)}"', label)

    def test_expected_failure_and_evidence_schema_are_fail_closed(self):
        expected_failure_steps = []
        for scenario in self.manifest["scenarios"]:
            for mode in ("quick", "qualification"):
                for step in scenario["modes"][mode]["steps"]:
                    if step["expected_failure"]:
                        expected_failure_steps.append((scenario["id"], step))
        self.assertTrue(expected_failure_steps)
        for scenario_id, step in expected_failure_steps:
            self.assertEqual(scenario_id, "tls-credential-invalid")
            self.assertNotIn(0, step["expected_exit_codes"])
            self.assertTrue(
                step["required_output_regex"].startswith("DRILL_EXPECTED_FAILURE")
            )

        schema = json.loads(
            runfile("docs/operations/drill-manifest.schema.json").read_text(
                encoding="utf-8"
            )
        )
        summary = json.loads(
            runfile("docs/operations/quick-drill-summary.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(schema["$id"], "mino://operations/drill-manifest/v1")
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(summary["schema_version"], 1)
        self.assertEqual(summary["mode"], "quick")
        self.assertEqual(
            set(summary["scenarios"]), set(self.manifest["required_scenarios"])
        )
        self.assertNotIn("logs", summary)
        self.assertNotIn("hostname", json.dumps(summary).lower())


if __name__ == "__main__":
    unittest.main()
