#!/usr/bin/env python3
"""Unit tests for Mino per-host SHM and cross-host TCP orchestration."""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.pipeline_comparison import mino_hybrid_runner as hybrid
from benchmarks.pipeline_comparison import pipeline_network_runner as network


class MinoHybridRunnerTest(unittest.TestCase):
    temporary: tempfile.TemporaryDirectory[str] | None = None
    root: Path | None = None

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        if self.temporary is not None:
            self.temporary.cleanup()

    @staticmethod
    def arguments() -> argparse.Namespace:
        return argparse.Namespace(
            profile="small",
            messages=100,
            warmup_messages=20,
            publish_interval_us=0,
            deadline_seconds=60,
            port_base=25000,
            channel_capacity=64,
            receive_batch_size=1,
            shm_binary_relative=hybrid.DEFAULT_SHM_BINARY,
            bridge_binary_relative=hybrid.DEFAULT_BRIDGE_BINARY,
            schema_descriptor_relative=network.DEFAULT_DESCRIPTOR,
            keep_remote_runtime=False,
        )

    @staticmethod
    def topology() -> dict[str, network.RoleHost]:
        return {
            role: network.RoleHost(
                role=role,
                ssh_host="host-a" if index < 3 else "host-b",
                data_address="10.0.0.11" if index < 3 else "10.0.0.12",
                workdir=Path("/srv/Mino"),
                environment={},
            )
            for index, role in enumerate(network.ROLES)
        }

    @staticmethod
    def bridge_document(
        args: argparse.Namespace,
        *,
        mode: str,
        boot_id: str,
        clock_mode: str = "independent-hosts",
    ) -> dict[str, object]:
        total = args.messages + args.warmup_messages
        source = mode == "source"
        return {
            "schema": hybrid.BRIDGE_SCHEMA,
            "run_id": "run-result",
            "edge": 2,
            "mode": mode,
            "validation": "structural",
            "profile": args.profile,
            "clock_mode": clock_mode,
            "clock": {
                "name": "CLOCK_MONOTONIC_RAW",
                "resolution_ns": 1,
                "boot_id": boot_id,
            },
            "compilation_mode": "opt",
            "receive_batch_size": args.receive_batch_size,
            "outcome": "success",
            "error": "",
            "counters": {
                "validation_calls": 0,
                "validation_payload_bytes": 0,
                "validation_thread_cpu_ns": 0,
            },
            "wire": {
                "data_frames_sent": total if source else 0,
                "data_frame_body_bytes_sent": total * 320 if source else 0,
                "data_frames_received": 0 if source else total,
                "data_frame_body_bytes_received": 0 if source else total * 320,
                "control_frames_sent": 0 if source else 1,
                "control_frame_body_bytes_sent": 0 if source else 128,
                "control_frames_received": 1 if source else 0,
                "control_frame_body_bytes_received": 128 if source else 0,
            },
        }

    def test_three_plus_three_maps_only_middle_edge_to_tcp(self) -> None:
        assert self.root is not None
        boot_ids = {
            role: "boot-a" if index < 3 else "boot-b"
            for index, role in enumerate(network.ROLES)
        }
        shm_names = hybrid._shm_names("run-hybrid", boot_ids)
        self.assertEqual(2, len(shm_names))
        self.assertNotEqual(shm_names["boot-a"], shm_names["boot-b"])

        processes, edges = hybrid.create_processes(
            self.arguments(), self.topology(), boot_ids, shm_names,
            self.root, "run-hybrid"
        )
        self.assertEqual(["shm", "shm", "tcp", "shm", "shm"],
                         [edge["transport"] for edge in edges])
        self.assertEqual(8, len(processes))
        bridges = [process for process in processes if process.name.startswith("bridge-")]
        self.assertEqual(
            ["bridge-edge-2-sink", "bridge-edge-2-source"],
            [process.name for process in bridges],
        )
        sink = bridges[0]
        source = bridges[1]
        self.assertEqual("25002", sink.command[sink.command.index("--port") + 1])
        self.assertEqual("0.0.0.0", sink.command[sink.command.index("--listen-address") + 1])
        self.assertEqual("10.0.0.12", source.command[source.command.index("--peer-address") + 1])
        self.assertEqual(shm_names["boot-b"], sink.command[sink.command.index("--shm-name") + 1])
        self.assertEqual(shm_names["boot-a"], source.command[source.command.index("--shm-name") + 1])
        for bridge in bridges:
            self.assertEqual(
                "structural",
                bridge.command[bridge.command.index("--bridge-validation") + 1],
            )
            self.assertEqual(
                "independent-hosts",
                bridge.command[bridge.command.index("--clock-mode") + 1],
            )
            self.assertEqual(
                "1",
                bridge.command[bridge.command.index("--receive-batch-size") + 1],
            )
            self.assertIsNotNone(bridge.result_remote)
            self.assertIsNotNone(bridge.result_local)
            self.assertEqual(
                str(bridge.result_remote),
                bridge.command[bridge.command.index("--output") + 1],
            )

    def test_bridge_result_validation_checks_topology_and_wire_counts(self) -> None:
        args = self.arguments()
        host = self.topology()["planning"]
        process = hybrid.ProcessRecord(
            name="bridge-edge-2-source",
            host=host,
            runtime_dir=Path("/tmp/bridge-source"),
            command=[],
            ready_name="ready",
            stdout_path=Path("/tmp/stdout"),
            stderr_path=Path("/tmp/stderr"),
        )
        process.process = mock.Mock()
        process.process.poll.return_value = 0
        total = args.messages + args.warmup_messages
        document = {
            "schema": hybrid.BRIDGE_SCHEMA,
            "run_id": "run-result",
            "edge": 2,
            "mode": "source",
            "validation": "structural",
            "profile": "small",
            "clock_mode": "independent-hosts",
            "clock": {
                "name": "CLOCK_MONOTONIC_RAW",
                "resolution_ns": 1,
                "boot_id": "boot-a",
            },
            "compilation_mode": "opt",
            "receive_batch_size": 1,
            "outcome": "success",
            "error": "",
            "counters": {
                "validation_calls": 0,
                "validation_payload_bytes": 0,
                "validation_thread_cpu_ns": 0,
            },
            "wire": {
                "data_frames_sent": total,
                "data_frame_body_bytes_sent": total * 320,
                "data_frames_received": 0,
                "data_frame_body_bytes_received": 0,
                "control_frames_sent": 0,
                "control_frame_body_bytes_sent": 0,
                "control_frames_received": 1,
                "control_frame_body_bytes_received": 128,
            },
        }
        self.assertEqual(
            document,
            hybrid.validate_bridge_result(
                document,
                args=args,
                process=process,
                run_id="run-result",
                same_host=False,
                expected_boot_id="boot-a",
            ),
        )
        corrupted = dict(document)
        corrupted["wire"] = dict(document["wire"])
        corrupted["wire"]["control_frames_received"] = 0
        with self.assertRaisesRegex(ValueError, "control_frames_received"):
            hybrid.validate_bridge_result(
                corrupted,
                args=args,
                process=process,
                run_id="run-result",
                same_host=False,
                expected_boot_id="boot-a",
            )

        sink_process = copy.copy(process)
        sink_process.name = "bridge-edge-2-sink"
        sink_process.host = self.topology()["control"]
        sink_document = self.bridge_document(
            args, mode="sink", boot_id="boot-b"
        )
        self.assertEqual(
            sink_document,
            hybrid.validate_bridge_result(
                sink_document,
                args=args,
                process=sink_process,
                run_id="run-result",
                same_host=False,
                expected_boot_id="boot-b",
            ),
        )
        self.assertEqual(
            args.messages + args.warmup_messages,
            sink_document["wire"]["data_frames_received"],
        )
        self.assertEqual(1, sink_document["wire"]["control_frames_sent"])

    def test_bridge_result_rejects_boot_mismatch_in_each_clock_mode(self) -> None:
        args = self.arguments()
        process = hybrid.ProcessRecord(
            name="bridge-edge-2-source",
            host=self.topology()["planning"],
            runtime_dir=Path("/tmp/bridge-source"),
            command=[],
            ready_name="ready",
            stdout_path=Path("/tmp/stdout"),
            stderr_path=Path("/tmp/stderr"),
        )
        process.process = mock.Mock()
        process.process.poll.return_value = 0
        for same_host, clock_mode in (
            (True, "same-host"),
            (False, "independent-hosts"),
        ):
            with self.subTest(clock_mode=clock_mode):
                document = self.bridge_document(
                    args,
                    mode="source",
                    boot_id="artifact-boot",
                    clock_mode=clock_mode,
                )
                with self.assertRaisesRegex(ValueError, "boot_id"):
                    hybrid.validate_bridge_result(
                        document,
                        args=args,
                        process=process,
                        run_id="run-result",
                        same_host=same_host,
                        expected_boot_id="preflight-boot",
                    )

    def test_collects_both_bridges_and_manifest_records_path_and_sha256(self) -> None:
        assert self.root is not None
        args = self.arguments()
        output = self.root / "output"
        remote = self.root / "remote"
        results = output / "results"
        remote.mkdir()
        results.mkdir(parents=True)
        topology = self.topology()
        records: list[hybrid.ProcessRecord] = []
        for mode, role, boot_id in (
            ("source", "planning", "boot-a"),
            ("sink", "control", "boot-b"),
        ):
            remote_result = remote / f"{mode}.json"
            local_result = results / f"bridge-edge-2-{mode}.json"
            remote_result.write_text(
                json.dumps(
                    self.bridge_document(args, mode=mode, boot_id=boot_id)
                ),
                encoding="utf-8",
            )
            process = hybrid.ProcessRecord(
                name=f"bridge-edge-2-{mode}",
                host=topology[role],
                runtime_dir=Path(f"/tmp/bridge-{mode}"),
                command=[],
                ready_name="ready",
                stdout_path=output / f"{mode}.stdout",
                stderr_path=output / f"{mode}.stderr",
                result_remote=remote_result,
                result_local=local_result,
            )
            process.process = mock.Mock()
            process.process.poll.return_value = 0
            records.append(process)

        def copy_result(
            _host: network.RoleHost, source: Path, destination: Path,
            _timeout: float,
        ) -> bool:
            destination.write_bytes(source.read_bytes())
            return True

        with mock.patch.object(
            network, "_remote_copy", side_effect=copy_result
        ):
            errors, sink = hybrid.collect_and_validate(
                args,
                records,
                {"planning": "boot-a", "control": "boot-b"},
                "run-result",
                False,
            )
        self.assertEqual([], errors)
        self.assertIsNone(sink)

        artifacts = hybrid._bridge_artifact_records(records, output)
        self.assertEqual(
            [
                "results/bridge-edge-2-source.json",
                "results/bridge-edge-2-sink.json",
            ],
            [artifact["result"]["path"] for artifact in artifacts],
        )
        for process, artifact in zip(records, artifacts):
            assert process.result_local is not None
            expected_sha = hashlib.sha256(
                process.result_local.read_bytes()
            ).hexdigest()
            self.assertEqual(expected_sha, artifact["result"]["sha256"])

        manifest_path = output / "manifest.json"
        network._write_json_atomic(
            manifest_path, {"bridge_artifacts": artifacts}
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(artifacts, manifest["bridge_artifacts"])

    def test_one_boot_degenerates_to_all_shm_without_bridges(self) -> None:
        assert self.root is not None
        topology = self.topology()
        boot_ids = {role: "one-boot" for role in network.ROLES}
        shm_names = hybrid._shm_names("run-local", boot_ids)
        processes, edges = hybrid.create_processes(
            self.arguments(), topology, boot_ids, shm_names,
            self.root, "run-local"
        )
        self.assertEqual(6, len(processes))
        self.assertTrue(all(edge["transport"] == "shm" for edge in edges))
        self.assertEqual(1, len({edge["source_shm"] for edge in edges}))

    def test_failed_setup_is_registered_for_shm_and_runtime_cleanup(self) -> None:
        args = self.arguments()
        boot_ids = {
            role: "boot-a" if index < 3 else "boot-b"
            for index, role in enumerate(network.ROLES)
        }
        shm_names = hybrid._shm_names("run-setup", boot_ids)
        failed = mock.Mock(returncode=9, stdout="partial", stderr="setup failed")
        with mock.patch.object(network, "_remote_mkdir"), mock.patch.object(
            hybrid, "_remote_control", return_value=failed
        ):
            errors, setups = hybrid._setup_segments(
                args, self.topology(), boot_ids, shm_names, "run-setup"
            )

        self.assertTrue(any("setup failed" in error for error in errors), errors)
        self.assertEqual(1, len(setups))
        self.assertEqual("boot-a", next(
            boot_id for boot_id, name in shm_names.items()
            if name == setups[0].shm_name
        ))

        cleaned = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(
            hybrid, "_remote_control", return_value=cleaned
        ) as remote_control, mock.patch.object(
            network, "_remote_cleanup"
        ) as remote_cleanup:
            cleanup_errors = hybrid._cleanup_segments(
                args, setups, "run-setup", False
            )
        self.assertEqual([], cleanup_errors)
        self.assertIn("cleanup", remote_control.call_args.args[1])
        remote_cleanup.assert_called_once_with(
            setups[0].host, setups[0].runtime_dir, mock.ANY
        )

    def test_timed_out_setup_is_still_registered(self) -> None:
        args = self.arguments()
        boot_ids = {role: "one-boot" for role in network.ROLES}
        shm_names = hybrid._shm_names("run-timeout", boot_ids)
        with mock.patch.object(network, "_remote_mkdir"), mock.patch.object(
            hybrid,
            "_remote_control",
            side_effect=subprocess.TimeoutExpired(["setup"], 1),
        ):
            errors, setups = hybrid._setup_segments(
                args, self.topology(), boot_ids, shm_names, "run-timeout"
            )
        self.assertEqual(1, len(setups))
        self.assertTrue(any("SHM setup failed" in error for error in errors), errors)

    def test_launch_remote_calls_use_current_global_remaining_deadline(self) -> None:
        assert self.root is not None
        args = self.arguments()
        args.deadline_seconds = 3
        host = network.RoleHost(
            role="perception",
            ssh_host="local",
            data_address="127.0.0.1",
            workdir=network.REPOSITORY_ROOT,
            environment={},
        )
        process_record = hybrid.ProcessRecord(
            name="perception",
            host=host,
            runtime_dir=self.root / "runtime",
            command=[],
            ready_name="mino-shm-perception.ready",
            stdout_path=self.root / "stdout.log",
            stderr_path=self.root / "stderr.log",
        )
        clock = [100.0]
        mkdir_timeouts: list[float] = []
        read_timeouts: list[float] = []
        start_timeouts: list[float] = []
        child = mock.Mock()
        child.poll.return_value = None

        def remote_mkdir(_host: network.RoleHost, _path: Path, timeout: float) -> None:
            mkdir_timeouts.append(timeout)

        def remote_read(_host: network.RoleHost, _path: Path, timeout: float) -> str:
            read_timeouts.append(timeout)
            clock[0] += 2.9
            return "run-deadline\n"

        def remote_start(
            _host: network.RoleHost,
            _path: Path,
            _run_id: str,
            timeout: float,
        ) -> None:
            start_timeouts.append(timeout)
            clock[0] += 0.2

        with mock.patch.object(hybrid.time, "monotonic", side_effect=lambda: clock[0]), mock.patch.object(
            network, "_remote_mkdir", side_effect=remote_mkdir
        ), mock.patch.object(
            network, "_remote_read", side_effect=remote_read
        ), mock.patch.object(
            network, "_remote_write_start", side_effect=remote_start
        ), mock.patch.object(
            hybrid.subprocess, "Popen", return_value=child
        ), mock.patch.object(hybrid, "terminate_processes"):
            errors = hybrid.launch_and_wait(
                args, [process_record], "run-deadline"
            )

        self.assertEqual([3.0], mkdir_timeouts)
        self.assertEqual([3.0], read_timeouts)
        self.assertEqual(1, len(start_timeouts))
        self.assertAlmostEqual(0.1, start_timeouts[0], places=6)
        self.assertTrue(any("deadline expired" in error for error in errors), errors)

    def test_manifest_write_failure_is_controlled(self) -> None:
        assert self.root is not None
        output = self.root / "manifest-failure"
        stderr = io.StringIO()
        with mock.patch.object(
            network, "load_topology", return_value=self.topology()
        ), mock.patch.object(
            network, "read_boot_ids", side_effect=RuntimeError("preflight failed")
        ), mock.patch.object(
            network,
            "_write_json_atomic",
            side_effect=network.ConfigurationError("cannot write manifest.json"),
        ), contextlib.redirect_stderr(stderr):
            code = hybrid.main(
                [
                    "--topology", str(self.root / "topology.json"),
                    "--output-dir", str(output),
                ]
            )
        self.assertEqual(1, code)
        self.assertIn("Mino hybrid output error", stderr.getvalue())
        self.assertIn("cannot write manifest.json", stderr.getvalue())

    def test_process_runtime_cleanup_errors_are_returned(self) -> None:
        assert self.root is not None
        host = self.topology()["perception"]
        process = hybrid.ProcessRecord(
            name="perception",
            host=host,
            runtime_dir=Path("/tmp/hybrid-runtime"),
            command=[],
            ready_name="ready",
            stdout_path=self.root / "stdout.log",
            stderr_path=self.root / "stderr.log",
        )
        with mock.patch.object(
            network, "_remote_cleanup", side_effect=RuntimeError("rm failed")
        ):
            errors = hybrid._cleanup_process_runtimes([process], False)
        self.assertEqual(
            ["perception: remote cleanup failed: rm failed"], errors
        )

    def test_qualified_default_channel_capacity_is_eight(self) -> None:
        args = hybrid.build_parser().parse_args(
            ["--topology", "/tmp/topology.json", "--output-dir", "/tmp/output"]
        )
        self.assertEqual(8, args.channel_capacity)
        self.assertEqual(1, args.receive_batch_size)

    def test_channel_capacity_must_be_power_of_two(self) -> None:
        args = self.arguments()
        args.channel_capacity = 63
        with self.assertRaisesRegex(network.ConfigurationError, "power of two"):
            hybrid._validate_args(args, self.topology())


if __name__ == "__main__":
    unittest.main()
