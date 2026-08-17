#!/usr/bin/env python3
"""Tests for local and remote-capable pipeline network orchestration."""

from __future__ import annotations

import argparse
import copy
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from benchmarks.pipeline_comparison import pipeline_network_runner as runner


FAKE_WORKER = r'''#!/usr/bin/env python3
import argparse
import json
import os
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
args, _ = parser.parse_known_args()

backend = "mino-tcp-canonical"
ready = args.runtime_dir / f"{backend}-{args.role}.ready"
temporary = ready.with_suffix(".tmp")
temporary.write_text(args.run_id + "\n", encoding="utf-8")
os.replace(temporary, ready)
deadline = time.monotonic() + args.deadline_seconds
start = args.runtime_dir / "start"
while not start.exists():
    if time.monotonic() >= deadline:
        raise SystemExit(9)
    time.sleep(0.005)
if start.read_text(encoding="utf-8") != args.run_id + "\n":
    raise SystemExit(10)

sink = args.role == "canbus"
latency = {
    "samples": args.messages if sink and args.clock_mode == "same-host" else 0,
    "p50": 10 if sink and args.clock_mode == "same-host" else 0,
    "p95": 20 if sink and args.clock_mode == "same-host" else 0,
    "p99": 30 if sink and args.clock_mode == "same-host" else 0,
    "p99_9": 40 if sink and args.clock_mode == "same-host" else 0,
    "max": 50 if sink and args.clock_mode == "same-host" else 0,
}
throughput_samples = (
    args.messages if args.clock_mode == "same-host" else max(0, args.messages - 1)
)
elapsed = 1000 if sink and throughput_samples > 0 else 0
result = {
    "schema": "mino.pipeline_e2e_benchmark.worker.v1",
    "backend": backend,
    "role": args.role,
    "profile": args.profile,
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
    "clock": {
        "name": "CLOCK_MONOTONIC_RAW",
        "resolution_ns": 1,
        "boot_id": "fake-boot",
    },
    "counts": {
        "offered": args.messages,
        "received": args.messages,
        "duplicate": 0,
        "out_of_order": 0,
        "corrupt": 0,
        "lost": 0,
    },
    "latency_ns": latency,
    "elapsed_ns": elapsed,
    "throughput_messages_per_second": (
        throughput_samples * 1_000_000_000.0 / elapsed if sink else 0.0
    ),
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


class PipelineNetworkRunnerTest(unittest.TestCase):
    temporary: tempfile.TemporaryDirectory[str]
    root: Path
    fake_worker: Path

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fake_worker = self.root / "fake_network_worker.py"
        self.fake_worker.write_text(FAKE_WORKER, encoding="utf-8")
        self.fake_worker.chmod(0o755)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def topology_document(
        self, *, second_boot_host: bool = False
    ) -> dict[str, Any]:
        roles = {}
        for index, role in enumerate(runner.ROLES):
            roles[role] = {
                "ssh_host": "local" if not second_boot_host or index < 3 else "host-b",
                "data_address": f"127.0.0.{index + 1}",
                "workdir": str(runner.REPOSITORY_ROOT),
                "environment": {},
            }
        return {"schema": runner.TOPOLOGY_SCHEMA, "roles": roles}

    def write_topology(self) -> Path:
        path = self.root / "topology.json"
        path.write_text(json.dumps(self.topology_document()), encoding="utf-8")
        return path

    @staticmethod
    def arguments() -> argparse.Namespace:
        return argparse.Namespace(
            backend="mino_tcp",
            profile="small",
            messages=3,
            warmup_messages=0,
            publish_interval_us=0,
            deadline_seconds=3,
            domain_id=73,
            history_depth=64,
            port_base=24000,
            zmq_hwm=64,
            binary_relative=None,
            schema_descriptor_relative=runner.DEFAULT_DESCRIPTOR,
            keep_remote_runtime=False,
        )

    def test_local_six_process_run_writes_qualified_manifest(self) -> None:
        output = self.root / "output"
        arguments = [
            "--topology",
            str(self.write_topology()),
            "--output-dir",
            str(output),
            "--backend",
            "mino_tcp",
            "--profile",
            "small",
            "--messages",
            "3",
            "--warmup-messages",
            "0",
            "--deadline-seconds",
            "3",
            "--binary-relative",
            str(self.fake_worker),
        ]
        fake_boot_ids = {role: "fake-boot" for role in runner.ROLES}
        with mock.patch.object(runner, "read_boot_ids", return_value=fake_boot_ids):
            code = runner.main(arguments)
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(0, code)
        self.assertEqual("passed", manifest["outcome"])
        self.assertTrue(manifest["one_way_latency_valid"])
        self.assertEqual("same-host", manifest["clock_mode"])
        self.assertEqual(6, len(manifest["workers"]))
        self.assertEqual(3, manifest["sink_metrics"]["latency_ns"]["samples"])

    def test_boot_ids_define_clock_domain_instead_of_ssh_alias(self) -> None:
        topology = {
            role: runner.RoleHost(
                role=role,
                ssh_host=f"alias-{index}",
                data_address=f"10.0.0.{index + 1}",
                workdir=runner.REPOSITORY_ROOT,
                environment={},
            )
            for index, role in enumerate(runner.ROLES)
        }
        completed = mock.Mock(returncode=0, stdout="same-boot\n", stderr="")
        with mock.patch.object(runner, "_remote_run", return_value=completed):
            boot_ids = runner.read_boot_ids(topology, 1)
        self.assertEqual({"same-boot"}, set(boot_ids.values()))
        self.assertFalse(runner.is_same_host(topology))

    def test_multi_host_mino_tcp_maps_each_edge_to_downstream_role(self) -> None:
        topology = {
            role: runner.RoleHost(
                role=role,
                ssh_host="host-a" if index < 3 else "host-b",
                data_address="10.0.0.11" if index < 3 else "10.0.0.12",
                workdir=Path("/srv/Mino"),
                environment={},
            )
            for index, role in enumerate(runner.ROLES)
        }
        output = self.root / "multi-host-workers"
        output.mkdir()
        workers = runner.create_workers(
            self.arguments(), topology, output, "run-multi", False
        )
        self.assertEqual(6, len(workers))
        for index, worker in enumerate(workers):
            command = worker.command
            listen_index = command.index("--listen-address") + 1
            peer_index = command.index("--peer-address") + 1
            self.assertEqual("0.0.0.0", command[listen_index])
            downstream = runner.ROLES[min(index + 1, len(runner.ROLES) - 1)]
            self.assertEqual(topology[downstream].data_address, command[peer_index])
        remote_worker = workers[3]
        self.assertIn("setsid sh -c", remote_worker.launcher_command[-1])
        self.assertIn("worker-control.pid", remote_worker.launcher_command[-1])

        completed = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(
            runner, "_remote_read", return_value="4242\n"
        ), mock.patch.object(
            runner, "_remote_run", return_value=completed
        ) as remote_run:
            runner.terminate_workers([remote_worker])
        self.assertEqual(2, remote_run.call_count)
        self.assertEqual(
            ["/bin/kill", "-TERM", "--", "-4242"],
            remote_run.call_args_list[0].args[1],
        )
        self.assertEqual(
            ["/bin/kill", "-KILL", "--", "-4242"],
            remote_run.call_args_list[1].args[1],
        )

    def test_protobuf_zmq_uses_ipc_within_boot_and_tcp_across_boots(self) -> None:
        args = self.arguments()
        args.backend = "protobuf_zmq"
        topology = {
            role: runner.RoleHost(
                role=role,
                ssh_host="host-a" if index < 3 else "host-b",
                data_address="10.0.0.11" if index < 3 else "10.0.0.12",
                workdir=Path("/srv/Mino"),
                environment={},
            )
            for index, role in enumerate(runner.ROLES)
        }
        boot_ids = {
            role: "boot-a" if index < 3 else "boot-b"
            for index, role in enumerate(runner.ROLES)
        }
        output = self.root / "protobuf-zmq-workers"
        output.mkdir()
        workers = runner.create_workers(
            args, topology, output, "run-zmq", False, boot_ids
        )
        self.assertEqual(workers[0].runtime_dir, workers[1].runtime_dir)
        self.assertEqual(workers[1].runtime_dir, workers[2].runtime_dir)
        self.assertNotEqual(workers[2].runtime_dir, workers[3].runtime_dir)
        self.assertEqual(workers[3].runtime_dir, workers[5].runtime_dir)

        def option(worker: runner.Worker, name: str) -> str:
            return worker.command[worker.command.index(name) + 1]

        self.assertEqual("ipc", option(workers[1], "--input-transport"))
        self.assertEqual("ipc", option(workers[1], "--output-transport"))
        self.assertEqual("tcp", option(workers[2], "--output-transport"))
        self.assertEqual("tcp", option(workers[3], "--input-transport"))
        self.assertEqual("ipc", option(workers[3], "--output-transport"))
        self.assertEqual("0.0.0.0", option(workers[2], "--listen-address"))
        self.assertEqual("0.0.0.0", option(workers[3], "--listen-address"))
        self.assertEqual("10.0.0.12", option(workers[2], "--peer-address"))
        self.assertEqual("10.0.0.11", option(workers[3], "--upstream-address"))
        self.assertEqual("64", option(workers[2], "--hwm"))

    def test_mino_tcp_rejects_non_numeric_ipv4_before_launch(self) -> None:
        topology = {
            role: runner.RoleHost(
                role=role,
                ssh_host="local",
                data_address="localhost" if role == "planning" else "127.0.0.1",
                workdir=runner.REPOSITORY_ROOT,
                environment={},
            )
            for role in runner.ROLES
        }
        output = self.root / "create-workers"
        output.mkdir()
        with self.assertRaises(runner.ConfigurationError):
            runner.create_workers(self.arguments(), topology, output, "run-1", True)

    def test_independent_host_sink_requires_empty_latency(self) -> None:
        args = self.arguments()
        host = runner.RoleHost(
            role="canbus",
            ssh_host="host-b",
            data_address="10.0.0.2",
            workdir=runner.REPOSITORY_ROOT,
            environment={},
        )
        runtime = Path("/tmp/mino-network-test-canbus")
        remote_result = runtime / "result.json"
        worker = runner.Worker(
            host=host,
            runtime_dir=runtime,
            remote_result=remote_result,
            local_result=self.root / "canbus.json",
            stdout_path=self.root / "stdout.log",
            stderr_path=self.root / "stderr.log",
            command=[],
            launcher_command=[],
        )
        document = self.valid_result(args, worker, same_host=False)
        validated = runner.validate_result(
            document,
            args=args,
            worker=worker,
            run_id="run-1",
            same_host=False,
            expected_boot_id="boot-b",
        )
        self.assertEqual(0, validated["latency_ns"]["samples"])
        invalid = copy.deepcopy(document)
        invalid["latency_ns"]["samples"] = args.messages
        invalid["latency_ns"]["p50"] = 1
        with self.assertRaisesRegex(ValueError, "independent-host sink latency"):
            runner.validate_result(
                invalid,
                args=args,
                worker=worker,
                run_id="run-1",
                same_host=False,
                expected_boot_id="boot-b",
            )

    def test_strict_worker_schema_rejects_missing_payload_bytes(self) -> None:
        args = self.arguments()
        host = runner.RoleHost(
            role="canbus",
            ssh_host="local",
            data_address="127.0.0.1",
            workdir=runner.REPOSITORY_ROOT,
            environment={},
        )
        runtime = Path("/tmp/mino-network-test-canbus")
        worker = runner.Worker(
            host=host,
            runtime_dir=runtime,
            remote_result=runtime / "result.json",
            local_result=self.root / "canbus.json",
            stdout_path=self.root / "stdout.log",
            stderr_path=self.root / "stderr.log",
            command=[],
            launcher_command=[],
        )
        document = self.valid_result(args, worker, same_host=True)
        del document["payload_bytes"]
        with self.assertRaisesRegex(ValueError, "worker result keys differ"):
            runner.validate_result(
                document,
                args=args,
                worker=worker,
                run_id="run-1",
                same_host=True,
                expected_boot_id="boot-a",
            )

    @staticmethod
    def valid_result(
        args: argparse.Namespace, worker: runner.Worker, *, same_host: bool
    ) -> dict[str, Any]:
        sink = worker.host.role == "canbus"
        comparable = sink and same_host
        throughput_samples = args.messages if same_host else args.messages - 1
        elapsed = 1000 if sink and throughput_samples > 0 else 0
        return {
            "schema": runner.WORKER_SCHEMA,
            "backend": runner.BACKEND_WORKER_NAMES[args.backend],
            "role": worker.host.role,
            "profile": args.profile,
            "configuration": {
                "messages": args.messages,
                "warmup_messages": args.warmup_messages,
                "publish_interval_us": args.publish_interval_us,
                "deadline_seconds": args.deadline_seconds,
                "clock_mode": "same-host" if same_host else "independent-hosts",
                "run_id": "run-1",
                "runtime_dir": str(worker.runtime_dir),
                "output": str(worker.remote_result),
            },
            "clock": {
                "name": "CLOCK_MONOTONIC_RAW",
                "resolution_ns": 1,
                "boot_id": "boot-a" if same_host else "boot-b",
            },
            "counts": {
                "offered": args.messages,
                "received": args.messages,
                "duplicate": 0,
                "out_of_order": 0,
                "corrupt": 0,
                "lost": 0,
            },
            "latency_ns": {
                "samples": args.messages if comparable else 0,
                "p50": 10 if comparable else 0,
                "p95": 20 if comparable else 0,
                "p99": 30 if comparable else 0,
                "p99_9": 40 if comparable else 0,
                "max": 50 if comparable else 0,
            },
            "elapsed_ns": elapsed,
            "throughput_messages_per_second": (
                throughput_samples * 1_000_000_000.0 / elapsed if sink else 0.0
            ),
            "payload_bytes": 256,
            "encoded_bytes": 320,
            "outcome": "success",
            "error": "",
            "backend_details": {},
        }


if __name__ == "__main__":
    unittest.main()
