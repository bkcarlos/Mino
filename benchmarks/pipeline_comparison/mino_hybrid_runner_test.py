#!/usr/bin/env python3
"""Unit tests for Mino per-host SHM and cross-host TCP orchestration."""

from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

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

    def test_qualified_default_channel_capacity_is_eight(self) -> None:
        args = hybrid.build_parser().parse_args(
            ["--topology", "/tmp/topology.json", "--output-dir", "/tmp/output"]
        )
        self.assertEqual(8, args.channel_capacity)

    def test_channel_capacity_must_be_power_of_two(self) -> None:
        args = self.arguments()
        args.channel_capacity = 63
        with self.assertRaisesRegex(network.ConfigurationError, "power of two"):
            hybrid._validate_args(args, self.topology())


if __name__ == "__main__":
    unittest.main()
