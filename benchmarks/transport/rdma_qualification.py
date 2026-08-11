#!/usr/bin/env python3
"""Fail-closed physical RDMA transport qualification role runner."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from benchmarks.transport import transport_qualification as contract

QUALIFICATION = "rdma"
QUALIFICATION_ID = "D6-06"
CASES = ("tcp", "udp", "rdma", "rdma-zero-copy")


def read(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise contract.QualificationError(f"required device fact is empty: {path}")
    return value


def device_facts(device: str) -> dict[str, Any]:
    root = Path("/sys/class/infiniband") / device
    if platform.system() != "Linux" or not root.is_dir():
        raise contract.QualificationError(f"real RDMA device is absent: {root}")
    backing = (root / "device").resolve(strict=True)
    if not str(backing).startswith("/sys/devices/"):
        raise contract.QualificationError("RDMA device is not backed by Linux sysfs hardware")
    subsystem = (root / "subsystem").resolve(strict=True).name
    if subsystem != "infiniband":
        raise contract.QualificationError(f"RDMA sysfs provider class is {subsystem!r}")
    ports: list[dict[str, str]] = []
    ports_root = root / "ports"
    if not ports_root.is_dir():
        raise contract.QualificationError(f"RDMA device has no ports directory: {device}")
    for port in sorted(ports_root.iterdir(), key=lambda candidate: candidate.name):
        state = read(port / "state")
        physical_state = read(port / "phys_state")
        link_layer = read(port / "link_layer")
        rate = read(port / "rate")
        if "ACTIVE" not in state.upper() or "LINKUP" not in physical_state.upper():
            raise contract.QualificationError(
                f"RDMA port {device}/{port.name} is not ACTIVE/LINKUP: {state!r}, {physical_state!r}"
            )
        ports.append(
            {
                "port": port.name,
                "state": state,
                "physical_state": physical_state,
                "link_layer": link_layer,
                "rate": rate,
            }
        )
    if not ports:
        raise contract.QualificationError(f"RDMA device has no physical ports: {device}")
    facts: dict[str, Any] = {
        "device": device,
        "sysfs_class": subsystem,
        "sysfs_path": str(root.resolve()),
        "backing_device_path": str(backing),
        "node_guid": read(root / "node_guid"),
        "node_type": read(root / "node_type"),
        "ports": ports,
    }
    driver = backing / "driver"
    facts["driver"] = driver.resolve(strict=True).name if driver.exists() else ""
    for name in ("vendor", "device", "class", "modalias", "uevent"):
        candidate = backing / name
        if candidate.is_file():
            facts[name] = read(candidate)
    if not facts["driver"]:
        raise contract.QualificationError("RDMA backing device has no bound kernel driver")
    return facts


def command_for_case(
    args: argparse.Namespace, benchmark: Path, plugin: Path, case_id: str, index: int, result_path: Path
) -> list[str]:
    command = [
        str(benchmark),
        f"--transport={case_id}",
        f"--role={args.role}",
        f"--address={args.server_address}",
        f"--port={args.base_port + index}",
        f"--iterations={args.iterations}",
        f"--payloads={args.payloads}",
        f"--output={result_path}",
    ]
    if case_id == "udp":
        local = args.server_address if args.role == "server" else args.client_address
        peer = args.client_address if args.role == "server" else args.server_address
        command.extend(
            [
                f"--udp-local-address={local}",
                f"--udp-local-port={args.base_port + index}",
                f"--udp-peer-address={peer}",
                f"--udp-peer-port={args.base_port + index}",
            ]
        )
    if case_id.startswith("rdma"):
        command.extend([f"--rdma-provider={plugin}", f"--rdma-device={args.device}"])
    return command


def run(args: argparse.Namespace) -> int:
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    repo = Path(args.repo).resolve()
    payloads = [int(value) for value in args.payloads.split(",")]
    local_address = args.server_address if args.role == "server" else args.client_address
    peer_address = args.client_address if args.role == "server" else args.server_address
    local_node = args.server_node if args.role == "server" else args.client_node
    peer_node = args.client_node if args.role == "server" else args.server_node
    local_domain = args.server_security_domain if args.role == "server" else args.client_security_domain
    peer_domain = args.client_security_domain if args.role == "server" else args.server_security_domain
    local = contract.endpoint_identity(local_address, args.base_port, local_node, local_domain)
    peer = contract.endpoint_identity(peer_address, args.base_port, peer_node, peer_domain)
    source, errors = contract.git_source(repo, args.expected_commit)
    errors.extend(contract.validate_run_binding(args.run_id, args.run_attempt, args.session_nonce))
    errors.extend(contract.validate_endpoint(args.server_address, args.base_port))
    errors.extend(contract.validate_endpoint(args.client_address, args.base_port))
    errors.extend(contract.identity_errors(local, peer))
    if platform.system() != "Linux":
        errors.append("physical RDMA qualification requires Linux")

    manifest = contract.role_manifest_base(
        qualification=QUALIFICATION,
        qualification_id=QUALIFICATION_ID,
        role=args.role,
        run_id=args.run_id,
        run_attempt=args.run_attempt,
        session_nonce=args.session_nonce,
        source=source,
        local=local,
        peer=peer,
        payloads=payloads,
        iterations=args.iterations,
        kinds=[],
    )
    benchmark: Path | None = None
    plugin: Path | None = None
    selected_policy: dict[str, Any] = {}
    try:
        benchmark = Path(args.benchmark).resolve(strict=True)
        plugin = Path(args.plugin).resolve(strict=True)
        if not benchmark.is_file() or not os.access(benchmark, os.X_OK):
            raise contract.QualificationError("benchmark is not an executable regular file")
        if not plugin.is_file() or not os.access(plugin, os.R_OK):
            raise contract.QualificationError("RDMA provider plugin is not a readable regular file")
        manifest["benchmark"] = {"name": benchmark.name, "sha256": contract.sha256(benchmark)}
        provider, provider_errors = contract.provider_evidence(
            plugin, QUALIFICATION, args.approved_plugin_sha256
        )
        manifest["provider"] = provider
        errors.extend(provider_errors)
        manifest["physical"] = device_facts(args.device)
        _, selected_policy = contract.load_policy(Path(args.sla_policy), QUALIFICATION)
        manifest["sla"]["policy"] = selected_policy
        manifest["sla"]["policy_sha256"] = contract.sha256(Path(args.sla_policy))
    except (OSError, contract.QualificationError) as error:
        errors.append(str(error))

    artifacts: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    cases: list[dict[str, Any]] = []
    if not errors and benchmark is not None and plugin is not None:
        for index, case_id in enumerate(CASES):
            result_path = output / f"{case_id}.jsonl"
            log_path = output / f"{case_id}.log"
            command = command_for_case(args, benchmark, plugin, case_id, index, result_path)
            try:
                return_code, elapsed_ns = contract.run_case(
                    command=command, log_path=log_path, timeout_seconds=args.timeout_seconds
                )
                if return_code != 0:
                    raise contract.QualificationError(
                        f"{case_id} benchmark failed with {return_code}; see {log_path}"
                    )
                log_artifact = contract.artifact(log_path, output)
                artifacts.append(log_artifact)
                rows: list[dict[str, Any]] = []
                result_artifact = None
                if args.role == "client":
                    result_artifact = contract.artifact(result_path, output)
                    artifacts.append(result_artifact)
                    rows, case_checks, case_errors = contract.validate_rows(
                        case_id=case_id,
                        rows=contract.parse_jsonl(result_path),
                        payloads=payloads,
                        iterations=args.iterations,
                        provider_provenance_value=manifest["provider"]["provenance"],
                        case_policy=selected_policy.get("cases", {}).get(case_id, {}),
                    )
                    checks.extend(case_checks)
                    errors.extend(case_errors)
                cases.append(
                    {
                        "case": case_id,
                        "elapsed_ns": elapsed_ns,
                        "log": log_artifact,
                        "result": result_artifact,
                        "rows": rows,
                    }
                )
            except (OSError, subprocess.SubprocessError, contract.QualificationError, json.JSONDecodeError) as error:
                errors.append(str(error))
                break
            if args.role == "client" and case_id != CASES[-1]:
                time.sleep(2)

    manifest["matrix"]["cases"] = cases
    manifest["artifacts"] = artifacts
    manifest["sla"]["checks"] = checks
    manifest["sla"]["passed"] = not errors and (args.role == "server" or bool(checks) and all(item["passed"] for item in checks))
    expected_artifacts = len(CASES) * (2 if args.role == "client" else 1)
    manifest["artifacts_complete"] = len(cases) == len(CASES) and len(artifacts) == expected_artifacts
    return contract.write_role_manifest(output, manifest, errors)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--approved-plugin-sha256", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--role", choices=("server", "client"), required=True)
    parser.add_argument("--server-address", required=True)
    parser.add_argument("--client-address", required=True)
    parser.add_argument("--server-node", type=int, required=True)
    parser.add_argument("--client-node", type=int, required=True)
    parser.add_argument("--server-security-domain", type=int, required=True)
    parser.add_argument("--client-security-domain", type=int, required=True)
    parser.add_argument("--base-port", type=int, required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--session-nonce", required=True)
    parser.add_argument("--sla-policy", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--payloads", default="128,1024,65536,1048576")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()
    if not 1024 <= args.base_port <= 65531:
        parser.error("base-port must leave four consecutive non-privileged ports")
    payloads = args.payloads.split(",")
    if payloads != [str(value) for value in contract.REQUIRED_PAYLOADS]:
        parser.error("qualification payloads must be exactly 128,1024,65536,1048576")
    if args.iterations < contract.MIN_ITERATIONS or args.timeout_seconds <= 0:
        parser.error("qualification requires at least 1000 iterations and a positive timeout")
    return args


if __name__ == "__main__":
    try:
        sys.exit(run(parse()))
    except (OSError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"rdma qualification failed before manifest finalization: {error}", file=sys.stderr)
        sys.exit(2)
