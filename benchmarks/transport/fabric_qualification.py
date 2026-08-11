#!/usr/bin/env python3
"""Fail-closed two-host qualification for physical IPCF, NTB, and CXL links."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from benchmarks.transport import transport_qualification as contract

QUALIFICATION = "fabric"
QUALIFICATION_ID = "D6-07"
REQUIRED_KINDS = ("ipcf", "ntb", "cxl")


def read(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise contract.QualificationError(f"required device fact is empty: {path}")
    return value


def physical_facts(device_path: Path, link_state_path: Path, kinds: Sequence[str]) -> dict[str, Any]:
    if platform.system() != "Linux":
        raise contract.QualificationError("physical Fabric qualification requires Linux")
    device = device_path.resolve(strict=True)
    link = link_state_path.resolve(strict=True)
    if not device.is_dir() or not str(device).startswith("/sys/devices/"):
        raise contract.QualificationError("Fabric device evidence must be a real /sys/devices directory")
    if not link.is_file() or not str(link).startswith("/sys/"):
        raise contract.QualificationError("Fabric link evidence must be a real sysfs file")
    state = read(link)
    tokens = set(re.findall(r"[A-Z]+", state.upper()))
    negative = {"DOWN", "INACTIVE", "OFFLINE", "DISABLED", "ERROR", "NOT"}
    positive = {"ACTIVE", "LINKUP", "ONLINE", "UP"}
    if tokens & negative or not tokens & positive:
        raise contract.QualificationError(f"physical Fabric link is not active: {state!r}")
    subsystem_path = device / "subsystem"
    subsystem = subsystem_path.resolve(strict=True).name if subsystem_path.exists() else ""
    class_value = read(device / "class") if (device / "class").is_file() else ""
    if not subsystem and not class_value:
        raise contract.QualificationError("Fabric sysfs device class/subsystem evidence is absent")
    driver_path = device / "driver"
    driver = driver_path.resolve(strict=True).name if driver_path.exists() else ""
    if not driver:
        raise contract.QualificationError("Fabric device has no bound kernel driver")
    facts: dict[str, Any] = {
        "device_path": str(device),
        "link_state_path": str(link),
        "link_state": state,
        "sysfs_subsystem": subsystem,
        "sysfs_class": class_value,
        "driver": driver,
        "validated_kinds": list(kinds),
    }
    for name in ("vendor", "device", "class", "numa_node", "modalias", "uevent"):
        candidate = device / name
        if candidate.is_file():
            facts[name] = read(candidate)
    return facts


def command_for_case(
    args: argparse.Namespace,
    benchmark: Path,
    plugin: Path,
    case_id: str,
    index: int,
    result_path: Path,
) -> list[str]:
    transport = "tcp" if case_id == "tcp" else "fabric"
    command = [
        str(benchmark),
        f"--transport={transport}",
        f"--role={args.role}",
        f"--address={args.server_address}",
        f"--port={args.base_port + index}",
        f"--payloads={args.payloads}",
        f"--iterations={args.iterations}",
        f"--output={result_path}",
    ]
    if transport == "fabric":
        kind = case_id.removeprefix("fabric-")
        local_node = args.host_a_node if args.role == "server" else args.host_b_node
        peer_node = args.host_b_node if args.role == "server" else args.host_a_node
        local_domain = (
            args.host_a_security_domain if args.role == "server" else args.host_b_security_domain
        )
        peer_domain = (
            args.host_b_security_domain if args.role == "server" else args.host_a_security_domain
        )
        command.extend(
            [
                f"--fabric-provider={plugin}",
                f"--fabric-device={args.device}",
                f"--fabric-kind={kind}",
                f"--fabric-domain={args.fabric_domain}",
                f"--fabric-channel={args.fabric_channel + index - 1}",
                f"--local-node={local_node}",
                f"--local-security-domain={local_domain}",
                f"--peer-node={peer_node}",
                f"--peer-security-domain={peer_domain}",
            ]
        )
    return command


def run(args: argparse.Namespace) -> int:
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    repo = Path(args.repo).resolve()
    payloads = [int(value) for value in args.payloads.split(",")]
    kinds = args.kinds.split(",")
    local_address = args.server_address if args.role == "server" else args.client_address
    peer_address = args.client_address if args.role == "server" else args.server_address
    local_node = args.host_a_node if args.role == "server" else args.host_b_node
    peer_node = args.host_b_node if args.role == "server" else args.host_a_node
    local_domain = args.host_a_security_domain if args.role == "server" else args.host_b_security_domain
    peer_domain = args.host_b_security_domain if args.role == "server" else args.host_a_security_domain
    local = contract.endpoint_identity(local_address, args.base_port, local_node, local_domain)
    peer = contract.endpoint_identity(peer_address, args.base_port, peer_node, peer_domain)
    source, errors = contract.git_source(repo, args.expected_commit)
    errors.extend(contract.validate_run_binding(args.run_id, args.run_attempt, args.session_nonce))
    errors.extend(contract.validate_endpoint(args.server_address, args.base_port))
    errors.extend(contract.validate_endpoint(args.client_address, args.base_port))
    errors.extend(contract.identity_errors(local, peer))

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
        kinds=kinds,
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
            raise contract.QualificationError("Fabric provider plugin is not a readable regular file")
        manifest["benchmark"] = {"name": benchmark.name, "sha256": contract.sha256(benchmark)}
        provider, provider_errors = contract.provider_evidence(
            plugin, QUALIFICATION, args.approved_plugin_sha256
        )
        manifest["provider"] = provider
        errors.extend(provider_errors)
        manifest["physical"] = physical_facts(
            Path(args.device_path), Path(args.link_state_path), kinds
        )
        _, selected_policy = contract.load_policy(Path(args.sla_policy), QUALIFICATION)
        if selected_policy.get("required_kinds") != list(REQUIRED_KINDS):
            raise contract.QualificationError("Fabric SLA policy must require IPCF, NTB, and CXL")
        manifest["sla"]["policy"] = selected_policy
        manifest["sla"]["policy_sha256"] = contract.sha256(Path(args.sla_policy))
    except (OSError, contract.QualificationError) as error:
        errors.append(str(error))

    case_ids = contract.expected_case_ids(QUALIFICATION, kinds)
    artifacts: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    cases: list[dict[str, Any]] = []
    if not errors and benchmark is not None and plugin is not None:
        for index, case_id in enumerate(case_ids):
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
            if args.role == "client" and case_id != case_ids[-1]:
                time.sleep(2)

    manifest["matrix"]["cases"] = cases
    manifest["artifacts"] = artifacts
    manifest["sla"]["checks"] = checks
    manifest["sla"]["passed"] = not errors and (args.role == "server" or bool(checks) and all(item["passed"] for item in checks))
    expected_artifacts = len(case_ids) * (2 if args.role == "client" else 1)
    manifest["artifacts_complete"] = len(cases) == len(case_ids) and len(artifacts) == expected_artifacts
    return contract.write_role_manifest(output, manifest, errors)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--approved-plugin-sha256", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--device-path", required=True)
    parser.add_argument("--link-state-path", required=True)
    parser.add_argument("--kinds", default="ipcf,ntb,cxl")
    parser.add_argument("--role", choices=("server", "client"), required=True)
    parser.add_argument("--server-address", required=True)
    parser.add_argument("--client-address", required=True)
    parser.add_argument("--base-port", type=int, required=True)
    parser.add_argument("--fabric-domain", type=int, required=True)
    parser.add_argument("--fabric-channel", type=int, required=True)
    parser.add_argument("--host-a-node", type=int, required=True)
    parser.add_argument("--host-b-node", type=int, required=True)
    parser.add_argument("--host-a-security-domain", type=int, required=True)
    parser.add_argument("--host-b-security-domain", type=int, required=True)
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
    if args.kinds.split(",") != list(REQUIRED_KINDS):
        parser.error("Fabric qualification requires the complete ordered kind matrix ipcf,ntb,cxl")
    if not 1024 <= args.base_port <= 65532:
        parser.error("base-port must leave four consecutive non-privileged ports")
    positive = (
        args.fabric_domain,
        args.fabric_channel,
        args.host_a_node,
        args.host_b_node,
        args.host_a_security_domain,
        args.host_b_security_domain,
        args.iterations,
        args.timeout_seconds,
    )
    if any(value <= 0 for value in positive):
        parser.error("Fabric IDs, iterations, and timeout must be positive")
    if args.iterations < contract.MIN_ITERATIONS:
        parser.error("Fabric qualification requires at least 1000 iterations")
    if args.host_a_node == args.host_b_node:
        parser.error("Fabric qualification hosts must have distinct node IDs")
    if args.host_a_security_domain == args.host_b_security_domain:
        parser.error("Fabric qualification must cross security domains")
    payloads = args.payloads.split(",")
    if payloads != [str(value) for value in contract.REQUIRED_PAYLOADS]:
        parser.error("qualification payloads must be exactly 128,1024,65536,1048576")
    return args


if __name__ == "__main__":
    try:
        sys.exit(run(parse()))
    except (OSError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"fabric qualification failed before manifest finalization: {error}", file=sys.stderr)
        sys.exit(2)
