#!/usr/bin/env python3
"""Unit and fake-worker tests for pipeline_comparison_runner."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import pipeline_comparison_runner as runner


FAKE_WORKER = r'''#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--role", required=True)
parser.add_argument("--profile", required=True)
parser.add_argument("--messages", type=int, required=True)
parser.add_argument("--warmup-messages", type=int, required=True)
parser.add_argument("--publish-interval-us", type=int, required=True)
parser.add_argument("--deadline-seconds", type=int, required=True)
parser.add_argument("--clock-mode", required=True)
parser.add_argument("--run-id", required=True)
parser.add_argument("--runtime-dir", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--domain-id")
parser.add_argument("--history-depth")
parser.add_argument("--cyclonedds-backend", action="store_true")
parser.add_argument("--hwm")
parser.add_argument("--operation", default="worker")
parser.add_argument("--shm-name")
parser.add_argument("--channel-capacity")
args, _ = parser.parse_known_args()

if args.cyclonedds_backend:
    backend = "cyclonedds-idl"
elif args.domain_id is not None:
    backend = "fastdds-idl"
elif args.hwm is not None:
    backend = "protobuf-zmq"
elif args.shm_name is not None:
    backend = "mino-shm"
else:
    raise SystemExit("cannot infer fake backend")

if args.operation != "worker":
    raise SystemExit(0)

mode = os.environ.get("FAKE_PIPELINE_MODE", "success")
if mode == "timeout" and args.role == "perception":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    Path(os.environ["FAKE_CHILD_PID_FILE"]).write_text(str(child.pid), encoding="utf-8")

ready = args.runtime_dir / f"{backend}-{args.role}.ready"
temporary = ready.with_suffix(".tmp")
ready_value = "wrong-run-id" if mode == "wrong_ready" else args.run_id
temporary.write_text(ready_value + "\n", encoding="utf-8")
os.replace(temporary, ready)

end = time.monotonic() + args.deadline_seconds + 2
while not (args.runtime_dir / "start").exists():
    if time.monotonic() >= end:
        raise SystemExit(9)
    time.sleep(0.005)

if mode == "timeout":
    time.sleep(60)

profile = "large" if mode == "wrong_profile" else args.profile
sink = args.role == "canbus"
latency = {
    "samples": args.messages if sink else 0,
    "p50": 10 if sink else 0,
    "p95": 20 if sink else 0,
    "p99": 30 if sink else 0,
    "p99_9": 40 if sink else 0,
    "max": 50 if sink else 0,
}
if mode == "nonmonotonic" and sink:
    latency["p95"] = 5
lost = 1 if mode == "loss" else 0
received = args.messages - lost
result = {
    "schema": "mino.pipeline_e2e_benchmark.worker.v1",
    "backend": backend,
    "role": args.role,
    "profile": profile,
    "configuration": {
        "messages": args.messages,
        "warmup_messages": args.warmup_messages,
        "publish_interval_us": args.publish_interval_us,
        "deadline_seconds": args.deadline_seconds,
        "clock_mode": args.clock_mode,
        "run_id": args.run_id,
        "runtime_dir": str(args.runtime_dir),
        "output": str(args.output),
    },
    "clock": {"name": "CLOCK_MONOTONIC_RAW", "resolution_ns": 1, "boot_id": "fake-boot"},
    "counts": {
        "offered": args.messages,
        "received": received,
        "duplicate": 0,
        "out_of_order": 0,
        "corrupt": 0,
        "lost": lost,
    },
    "latency_ns": latency,
    "elapsed_ns": 1000 if sink else 0,
    "throughput_messages_per_second": args.messages * 1000000.0 if sink else 0.0,
    "payload_bytes": 256,
    "encoded_bytes": 320,
    "outcome": "success",
    "error": "",
    "backend_details": {"fake": True},
}
args.output.parent.mkdir(parents=True, exist_ok=True)
temporary_output = args.output.with_suffix(".tmp")
temporary_output.write_text(json.dumps(result), encoding="utf-8")
os.replace(temporary_output, args.output)
'''


class PipelineComparisonRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fake_worker = self.root / "fake_pipeline_worker.py"
        self.fake_worker.write_text(FAKE_WORKER, encoding="utf-8")
        self.fake_worker.chmod(0o755)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def runner_arguments(self, output: Path, *extra: str) -> list[str]:
        return [
            "--output-dir",
            str(output),
            "--profiles",
            "small",
            "--rounds",
            "1",
            "--small-messages",
            "3",
            "--warmup-ratio",
            "0.0",
            "--deadline-seconds",
            "2",
            "--fastdds-binary",
            str(self.fake_worker),
            "--cyclonedds-binary",
            str(self.fake_worker),
            "--protobuf-zmq-binary",
            str(self.fake_worker),
            "--mino-shm-binary",
            str(self.fake_worker),
            "--fail-fast",
            *extra,
        ]

    def run_fake(self, mode: str, output_name: str) -> tuple[int, dict]:
        output = self.root / output_name
        with mock.patch.dict(os.environ, {"FAKE_PIPELINE_MODE": mode}, clear=False):
            code = runner.main(self.runner_arguments(output))
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        return code, manifest

    def test_backend_order_is_deterministic_and_rotates_each_round(self) -> None:
        first = runner.backend_order(12345, "small", 0)
        self.assertEqual(first, runner.backend_order(12345, "small", 0))
        self.assertEqual(first[1:] + first[:1], runner.backend_order(12345, "small", 1))
        self.assertEqual(first[2:] + first[:2], runner.backend_order(12345, "small", 2))
        self.assertEqual(set(runner.BACKEND_NAMES), set(first))

    def test_successful_fake_matrix_passes_with_complete_manifest(self) -> None:
        output = self.root / "success"
        with mock.patch.dict(os.environ, {"FAKE_PIPELINE_MODE": "success"}, clear=False):
            code = runner.main(self.runner_arguments(output))
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(0, code)
        self.assertEqual("passed", manifest["outcome"])
        self.assertEqual([], manifest["errors"])
        self.assertEqual(4, len(manifest["runs"]))
        self.assertTrue(all(run["sink_metrics"] for run in manifest["runs"]))
        self.assertTrue(
            all(len(run["workers"]) == len(runner.ROLES) for run in manifest["runs"])
        )

    def test_wrong_ready_content_fails_and_manifest_is_written(self) -> None:
        code, manifest = self.run_fake("wrong_ready", "wrong-ready")
        self.assertEqual(1, code)
        self.assertEqual("failed", manifest["outcome"])
        self.assertTrue(
            any("mismatched run-id" in error for error in manifest["errors"]),
            manifest["errors"],
        )
        self.assertEqual(runner.MANIFEST_SCHEMA, manifest["schema"])

    def test_fake_worker_json_rejections_are_preserved_in_manifest(self) -> None:
        expected_fragments = {
            "loss": "counts.offered and counts.received must equal messages",
            "wrong_profile": "profile must be 'small'",
            "nonmonotonic": "latency percentiles must be monotonic",
        }
        for mode, fragment in expected_fragments.items():
            with self.subTest(mode=mode):
                code, manifest = self.run_fake(mode, mode)
                self.assertEqual(1, code)
                self.assertEqual("failed", manifest["outcome"])
                self.assertTrue(
                    any(fragment in error for error in manifest["errors"]),
                    manifest["errors"],
                )
                run = manifest["runs"][0]
                self.assertEqual(6, len(run["workers"]))
                for worker in run["workers"]:
                    self.assertFalse(Path(worker["stdout"]["path"]).is_absolute())
                    self.assertFalse(Path(worker["stderr"]["path"]).is_absolute())
                    self.assertFalse(Path(worker["result"]["path"]).is_absolute())

    def test_validate_worker_result_rejects_loss_wrong_profile_and_percentiles(self) -> None:
        valid = self.valid_result()
        self.assertEqual(valid, self.validate(valid))

        mutations = []
        loss = copy.deepcopy(valid)
        loss["counts"]["lost"] = 1
        mutations.append((loss, "counts.lost must be zero"))
        wrong_profile = copy.deepcopy(valid)
        wrong_profile["profile"] = "large"
        mutations.append((wrong_profile, "profile must be 'small'"))
        percentiles = copy.deepcopy(valid)
        percentiles["latency_ns"]["p95"] = 5
        mutations.append((percentiles, "latency percentiles must be monotonic"))
        for document, error in mutations:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    self.validate(document)

    def test_timeout_terminates_spawned_process_group(self) -> None:
        output = self.root / "timeout"
        child_pid_path = self.root / "child.pid"
        environment = {
            "FAKE_PIPELINE_MODE": "timeout",
            "FAKE_CHILD_PID_FILE": str(child_pid_path),
        }
        arguments = self.runner_arguments(output)
        deadline_index = arguments.index("--deadline-seconds") + 1
        arguments[deadline_index] = "1"
        with mock.patch.dict(os.environ, environment, clear=False), mock.patch.object(
            runner, "TERMINATION_GRACE_SECONDS", 0.1
        ):
            code = runner.main(arguments)
        self.assertEqual(1, code)
        self.assertTrue((output / "manifest.json").is_file())
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))
        deadline = time.monotonic() + 2
        while self.process_is_running(child_pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertFalse(self.process_is_running(child_pid))

    def test_output_directory_gate_accepts_absent_or_empty_and_rejects_nonempty(self) -> None:
        absent = self.root / "absent"
        self.assertEqual(absent.resolve(), runner.prepare_output_directory(absent))
        empty = self.root / "empty"
        empty.mkdir()
        self.assertEqual(empty.resolve(), runner.prepare_output_directory(empty))
        nonempty = self.root / "nonempty"
        nonempty.mkdir()
        (nonempty / "keep.txt").write_text("user data", encoding="utf-8")
        with self.assertRaises(runner.RunnerConfigurationError):
            runner.prepare_output_directory(nonempty)
        self.assertEqual("user data", (nonempty / "keep.txt").read_text(encoding="utf-8"))

    @staticmethod
    def process_is_running(pid: int) -> bool:
        completed = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        state = completed.stdout.strip()
        return bool(state) and not state.startswith("Z")

    @staticmethod
    def valid_result() -> dict:
        return {
            "schema": runner.WORKER_SCHEMA,
            "backend": "fastdds-idl",
            "role": "canbus",
            "profile": "small",
            "configuration": {
                "messages": 3,
                "warmup_messages": 0,
                "publish_interval_us": 0,
                "deadline_seconds": 2,
                "clock_mode": "same-host",
                "run_id": "run-1",
                "runtime_dir": "/tmp/mino-pipeline-test",
                "output": "/tmp/result.json",
            },
            "clock": {"name": "CLOCK_MONOTONIC_RAW", "resolution_ns": 1, "boot_id": "boot"},
            "counts": {
                "offered": 3,
                "received": 3,
                "duplicate": 0,
                "out_of_order": 0,
                "corrupt": 0,
                "lost": 0,
            },
            "latency_ns": {"samples": 3, "p50": 10, "p95": 20, "p99": 30, "p99_9": 40, "max": 50},
            "elapsed_ns": 100,
            "throughput_messages_per_second": 30_000_000.0,
            "payload_bytes": 256,
            "encoded_bytes": 320,
            "outcome": "success",
            "error": "",
            "backend_details": {},
        }

    @staticmethod
    def validate(document: dict) -> dict:
        return runner.validate_worker_result(
            document,
            expected_backend="fastdds-idl",
            expected_role="canbus",
            expected_profile="small",
            expected_run_id="run-1",
            expected_messages=3,
            expected_warmup_messages=0,
            expected_publish_interval_us=0,
            expected_deadline_seconds=2,
        )


if __name__ == "__main__":
    unittest.main()
