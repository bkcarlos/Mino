#!/usr/bin/env python3
"""Run Mino with host-local SHM edges and cross-host canonical TCP bridges."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import ipaddress
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from benchmarks.pipeline_comparison import pipeline_network_runner as network
from benchmarks.pipeline_comparison.pipeline_comparison_runner import validate_worker_result

MANIFEST_SCHEMA = "mino.pipeline_hybrid_benchmark.manifest.v1"
WORKER_BACKEND = "mino-shm"
DEFAULT_SHM_BINARY = "bazel-bin/benchmarks/pipeline_comparison/mino_shm_pipeline"
DEFAULT_BRIDGE_BINARY = "bazel-bin/benchmarks/pipeline_comparison/mino_shm_tcp_bridge"
POLL_SECONDS = 0.05


@dataclass
class ProcessRecord:
    name: str
    host: network.RoleHost
    runtime_dir: Path
    command: list[str]
    ready_name: str
    stdout_path: Path
    stderr_path: Path
    result_remote: Optional[Path] = None
    result_local: Optional[Path] = None
    process: Optional[subprocess.Popen[Any]] = None
    stdout_handle: Any = None
    stderr_handle: Any = None


def _bounded_int(name: str, minimum: int, maximum: int):
    return network._bounded_int(name, minimum, maximum)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile", choices=network.PROFILES, default="small")
    parser.add_argument("--messages", type=_bounded_int("--messages", 1, 1_000_000_000), default=1000)
    parser.add_argument("--warmup-messages", type=_bounded_int("--warmup-messages", 0, 1_000_000_000), default=100)
    parser.add_argument("--publish-interval-us", type=_bounded_int("--publish-interval-us", 0, 60_000_000), default=0)
    parser.add_argument("--deadline-seconds", type=_bounded_int("--deadline-seconds", 1, 86_400), default=120)
    parser.add_argument("--port-base", type=_bounded_int("--port-base", 1, 65_531), default=25_000)
    parser.add_argument("--channel-capacity", type=_bounded_int("--channel-capacity", 2, 4096), default=8)
    parser.add_argument("--shm-binary-relative", default=DEFAULT_SHM_BINARY)
    parser.add_argument("--bridge-binary-relative", default=DEFAULT_BRIDGE_BINARY)
    parser.add_argument("--schema-descriptor-relative", default=network.DEFAULT_DESCRIPTOR)
    parser.add_argument("--keep-remote-runtime", action="store_true")
    return parser


def _validate_args(args: argparse.Namespace, topology: Mapping[str, network.RoleHost]) -> None:
    if args.channel_capacity & (args.channel_capacity - 1):
        raise network.ConfigurationError("--channel-capacity must be a power of two")
    for role in network.ROLES:
        try:
            ipaddress.IPv4Address(topology[role].data_address)
        except ipaddress.AddressValueError as exc:
            raise network.ConfigurationError(
                f"roles.{role}.data_address must be a numeric IPv4 address"
            ) from exc
        for relative, name in (
            (args.shm_binary_relative, "--shm-binary-relative"),
            (args.bridge_binary_relative, "--bridge-binary-relative"),
            (args.schema_descriptor_relative, "--schema-descriptor-relative"),
        ):
            if Path(relative).is_absolute() or "\0" in relative:
                raise network.ConfigurationError(f"{name} must be repository-relative")


def _environment_command(host: network.RoleHost, command: Sequence[str]) -> list[str]:
    return ["env", *[f"{key}={value}" for key, value in sorted(host.environment.items())], *command]


def _remote_control(host: network.RoleHost, command: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return network._remote_run(host, _environment_command(host, command), timeout)


def _common_arguments(
    args: argparse.Namespace,
    role: str,
    run_id: str,
    runtime_dir: Path,
    output: Path,
    same_host: bool,
) -> list[str]:
    return network._common_arguments(args, role, run_id, runtime_dir, output, same_host)


def _shm_names(run_id: str, boot_ids: Mapping[str, str]) -> dict[str, str]:
    run_token = hashlib.sha256(run_id.encode()).hexdigest()[:12]
    return {
        boot_id: "/mino_hybrid_" + run_token + "_" + hashlib.sha256(boot_id.encode()).hexdigest()[:12]
        for boot_id in set(boot_ids.values())
    }


def _representatives(
    topology: Mapping[str, network.RoleHost], boot_ids: Mapping[str, str]
) -> dict[str, network.RoleHost]:
    result: dict[str, network.RoleHost] = {}
    for role in network.ROLES:
        result.setdefault(boot_ids[role], topology[role])
    return result


def _setup_segments(
    args: argparse.Namespace,
    topology: Mapping[str, network.RoleHost],
    boot_ids: Mapping[str, str],
    shm_names: Mapping[str, str],
    run_id: str,
) -> tuple[list[str], list[tuple[network.RoleHost, str, Path]]]:
    errors: list[str] = []
    setups: list[tuple[network.RoleHost, str, Path]] = []
    token = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    for index, (boot_id, host) in enumerate(_representatives(topology, boot_ids).items()):
        runtime = Path("/tmp") / f"mino-hybrid-{token}-setup-{index}"
        try:
            network._remote_mkdir(host, runtime, 10)
            command = [
                str(host.workdir / args.shm_binary_relative),
                *_common_arguments(args, "perception", run_id, runtime, runtime / "setup.json", len(set(boot_ids.values())) == 1),
                "--operation", "setup",
                "--shm-name", shm_names[boot_id],
                "--channel-capacity", str(args.channel_capacity),
            ]
            completed = _remote_control(host, command, min(args.deadline_seconds, 60))
            (runtime / "setup.stdout.log").write_text(completed.stdout, encoding="utf-8") if host.local else None
            if completed.returncode != 0:
                errors.append(f"SHM setup failed for {host.ssh_host}: {completed.stderr.strip()}")
                break
            setups.append((host, shm_names[boot_id], runtime))
        except (OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
            errors.append(f"SHM setup failed for {host.ssh_host}: {exc}")
            break
    return errors, setups


def _cleanup_segments(
    args: argparse.Namespace,
    setups: Sequence[tuple[network.RoleHost, str, Path]],
    run_id: str,
    same_host: bool,
) -> list[str]:
    errors: list[str] = []
    for host, shm_name, runtime in reversed(setups):
        command = [
            str(host.workdir / args.shm_binary_relative),
            *_common_arguments(args, "perception", run_id, runtime, runtime / "cleanup.json", same_host),
            "--operation", "cleanup",
            "--shm-name", shm_name,
            "--channel-capacity", str(args.channel_capacity),
        ]
        try:
            completed = _remote_control(host, command, min(args.deadline_seconds, 30))
            if completed.returncode != 0:
                errors.append(f"SHM cleanup failed for {host.ssh_host}: {completed.stderr.strip()}")
        except (OSError, subprocess.TimeoutExpired) as exc:
            errors.append(f"SHM cleanup failed for {host.ssh_host}: {exc}")
        if not args.keep_remote_runtime:
            network._remote_cleanup(host, runtime, 10)
    return errors


def _make_process(
    *, name: str, host: network.RoleHost, runtime: Path, command: list[str],
    ready_name: str, logs: Path, result_remote: Optional[Path] = None,
    result_local: Optional[Path] = None,
) -> ProcessRecord:
    return ProcessRecord(
        name=name, host=host, runtime_dir=runtime, command=command,
        ready_name=ready_name, stdout_path=logs / f"{name}.stdout.log",
        stderr_path=logs / f"{name}.stderr.log", result_remote=result_remote,
        result_local=result_local,
    )


def create_processes(
    args: argparse.Namespace,
    topology: Mapping[str, network.RoleHost],
    boot_ids: Mapping[str, str],
    shm_names: Mapping[str, str],
    output: Path,
    run_id: str,
) -> tuple[list[ProcessRecord], list[dict[str, Any]]]:
    logs = output / "logs"
    results = output / "results"
    logs.mkdir()
    results.mkdir()
    token = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    same_host = len(set(boot_ids.values())) == 1
    workers: list[ProcessRecord] = []
    for role in network.ROLES:
        host = topology[role]
        runtime = Path("/tmp") / f"mino-hybrid-{token}-{role}"
        result_remote = runtime / "result.json"
        command = [
            str(host.workdir / args.shm_binary_relative),
            *_common_arguments(args, role, run_id, runtime, result_remote, same_host),
            "--operation", "worker",
            "--shm-name", shm_names[boot_ids[role]],
            "--channel-capacity", str(args.channel_capacity),
        ]
        workers.append(_make_process(
            name=role, host=host, runtime=runtime, command=command,
            ready_name=f"mino-shm-{role}.ready", logs=logs,
            result_remote=result_remote, result_local=results / f"{role}.json",
        ))

    bridges: list[ProcessRecord] = []
    edges: list[dict[str, Any]] = []
    for edge, (source_role, sink_role) in enumerate(zip(network.ROLES, network.ROLES[1:])):
        source = topology[source_role]
        sink = topology[sink_role]
        local = boot_ids[source_role] == boot_ids[sink_role]
        record: dict[str, Any] = {
            "edge": edge,
            "source_role": source_role,
            "sink_role": sink_role,
            "source_boot_id": boot_ids[source_role],
            "sink_boot_id": boot_ids[sink_role],
            "source_shm": shm_names[boot_ids[source_role]],
            "sink_shm": shm_names[boot_ids[sink_role]],
            "transport": "shm" if local else "tcp",
            "channel": f"edge-{edge}",
            "schema": "mino.benchmarks.pipeline.AutonomyPipelineFrame",
            "generated_type": "AutonomyPipelineFrame",
        }
        if local:
            edges.append(record)
            continue
        record.update({
            "port": args.port_base + edge,
            "source_address": source.data_address,
            "sink_address": sink.data_address,
            "descriptor_relative": args.schema_descriptor_relative,
        })
        edges.append(record)
        for mode, host, shm_name in (
            ("sink", sink, shm_names[boot_ids[sink_role]]),
            ("source", source, shm_names[boot_ids[source_role]]),
        ):
            name = f"bridge-edge-{edge}-{mode}"
            runtime = Path("/tmp") / f"mino-hybrid-{token}-{name}"
            command = [
                str(host.workdir / args.bridge_binary_relative),
                "--mode", mode,
                "--profile", args.profile,
                "--messages", str(args.messages),
                "--warmup-messages", str(args.warmup_messages),
                "--deadline-seconds", str(args.deadline_seconds),
                "--run-id", run_id,
                "--runtime-dir", str(runtime),
                "--shm-name", shm_name,
                "--edge", str(edge),
                "--port", str(args.port_base + edge),
                "--schema-descriptor", str(host.workdir / args.schema_descriptor_relative),
            ]
            command += ["--listen-address", "0.0.0.0"] if mode == "sink" else ["--peer-address", sink.data_address]
            bridges.append(_make_process(
                name=name, host=host, runtime=runtime, command=command,
                ready_name=f"mino-shm-tcp-bridge-{mode}.ready", logs=logs,
            ))
    # TCP listeners must exist before connector construction.
    bridges.sort(key=lambda process: 0 if process.name.endswith("-sink") else 1)
    return [*bridges, *reversed(workers)], edges


def _launcher(process: ProcessRecord) -> list[str]:
    command = _environment_command(process.host, process.command)
    if process.host.local:
        return command
    pid_path = process.runtime_dir / "process.pid"
    script = (
        "umask 077; printf '%s\\n' \"$$\" > " + shlex.quote(str(pid_path)) +
        "; exec " + shlex.join(command)
    )
    return [
        *network._ssh_prefix(process.host),
        "cd " + shlex.quote(str(process.host.workdir)) +
        " && exec setsid sh -c " + shlex.quote(script),
    ]


def _remote_pid(process: ProcessRecord) -> Optional[int]:
    if process.host.local:
        return None
    try:
        content = network._remote_read(process.host, process.runtime_dir / "process.pid", 2)
    except (OSError, subprocess.TimeoutExpired):
        return None
    text = content.strip() if content else ""
    return int(text) if text.isascii() and text.isdecimal() and 1 < int(text) <= 2_147_483_647 else None


def terminate_processes(processes: Sequence[ProcessRecord]) -> None:
    remote = [(process, pid) for process in processes if (pid := _remote_pid(process)) is not None]
    for process, pid in remote:
        try:
            network._remote_run(process.host, ["/bin/kill", "-TERM", "--", f"-{pid}"], 3)
        except (OSError, subprocess.TimeoutExpired):
            pass
    active = [process.process for process in processes if process.process is not None and process.process.poll() is None]
    for child in active:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 1.0
    while any(child.poll() is None for child in active) and time.monotonic() < deadline:
        time.sleep(0.02)
    for process, pid in remote:
        try:
            network._remote_run(process.host, ["/bin/kill", "-KILL", "--", f"-{pid}"], 3)
        except (OSError, subprocess.TimeoutExpired):
            pass
    for child in active:
        if child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def launch_and_wait(args: argparse.Namespace, processes: list[ProcessRecord], run_id: str) -> list[str]:
    errors: list[str] = []
    deadline = time.monotonic() + args.deadline_seconds
    completed = False
    try:
        for process in processes:
            network._remote_mkdir(process.host, process.runtime_dir, min(10, args.deadline_seconds))
            process.stdout_handle = process.stdout_path.open("wb")
            process.stderr_handle = process.stderr_path.open("wb")
            process.process = subprocess.Popen(
                _launcher(process), cwd=process.host.workdir if process.host.local else None,
                stdout=process.stdout_handle, stderr=process.stderr_handle,
                start_new_session=True,
            )
            # Give a listening bridge a small deterministic opportunity to bind before its connector.
            if process.name.endswith("-sink"):
                time.sleep(0.05)

        pending = {process.name: process for process in processes}
        while pending:
            for name, process in tuple(pending.items()):
                if process.process is not None and process.process.poll() is not None:
                    errors.append(f"{name} exited with {process.process.returncode} before readiness")
                    return errors
                content = network._remote_read(process.host, process.runtime_dir / process.ready_name, 5)
                if content == run_id + "\n":
                    del pending[name]
                elif content is not None:
                    errors.append(f"{name} readiness run-id mismatch")
                    return errors
            if time.monotonic() >= deadline:
                errors.append("deadline expired waiting for hybrid readiness")
                return errors
            time.sleep(POLL_SECONDS)

        # All transport endpoints and consumers are ready; source starts last.
        for process in processes:
            if process.name != "perception":
                network._remote_write_start(process.host, process.runtime_dir / "start", run_id, 10)
        perception = next(process for process in processes if process.name == "perception")
        network._remote_write_start(perception.host, perception.runtime_dir / "start", run_id, 10)

        active = list(processes)
        while active:
            for process in tuple(active):
                assert process.process is not None
                code = process.process.poll()
                if code is None:
                    continue
                active.remove(process)
                if code != 0:
                    errors.append(f"{process.name} exited with {code}")
            if errors:
                return errors
            if time.monotonic() >= deadline:
                errors.append("deadline expired waiting for hybrid processes")
                return errors
            time.sleep(POLL_SECONDS)
        completed = True
    except (OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
        errors.append(f"hybrid orchestration failed: {exc}")
    finally:
        if errors or not completed:
            terminate_processes(processes)
        for process in processes:
            if process.stdout_handle is not None:
                process.stdout_handle.close()
            if process.stderr_handle is not None:
                process.stderr_handle.close()
    return errors


def collect_and_validate(
    args: argparse.Namespace,
    processes: Sequence[ProcessRecord],
    boot_ids: Mapping[str, str],
    run_id: str,
    same_host: bool,
) -> tuple[list[str], Optional[dict[str, Any]]]:
    errors: list[str] = []
    sink: Optional[dict[str, Any]] = None
    for process in processes:
        if process.result_remote is None or process.result_local is None:
            continue
        try:
            copied = network._remote_copy(process.host, process.result_remote, process.result_local, min(30, args.deadline_seconds))
        except (OSError, subprocess.TimeoutExpired):
            copied = False
        if not copied:
            errors.append(f"{process.name}: could not collect result JSON")
            continue
        try:
            document = json.loads(process.result_local.read_text(encoding="utf-8"))
            validated = validate_worker_result(
                document,
                expected_backend=WORKER_BACKEND,
                expected_role=process.name,
                expected_profile=args.profile,
                expected_run_id=run_id,
                expected_messages=args.messages,
                expected_warmup_messages=args.warmup_messages,
                expected_publish_interval_us=args.publish_interval_us,
                expected_deadline_seconds=args.deadline_seconds,
                expected_clock_mode="same-host" if same_host else "independent-hosts",
                expected_runtime_directory=process.runtime_dir,
                expected_output=process.result_remote,
                expect_sink_latency=same_host,
            )
            if validated["clock"]["boot_id"] != boot_ids[process.name]:
                raise ValueError("clock.boot_id differs from topology preflight")
            if process.name == "canbus":
                sink = {
                    "latency_ns": validated["latency_ns"] if same_host else None,
                    "elapsed_ns": validated["elapsed_ns"],
                    "throughput_messages_per_second": validated["throughput_messages_per_second"],
                    "payload_bytes": validated["payload_bytes"],
                    "encoded_bytes": validated["encoded_bytes"],
                    "backend_details": validated["backend_details"],
                }
        except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            errors.append(f"{process.name}: invalid result: {exc}")
    if sink is None:
        errors.append("validated canbus result is missing")
    return errors, sink


def _process_records(processes: Sequence[ProcessRecord], output: Path) -> list[dict[str, Any]]:
    return [{
        "name": process.name,
        "role": process.host.role,
        "ssh_host": process.host.ssh_host,
        "data_address": process.host.data_address,
        "workdir": str(process.host.workdir),
        "runtime_dir": str(process.runtime_dir),
        "command": process.command,
        "exit_code": process.process.poll() if process.process is not None else None,
        "stdout": network._artifact(process.stdout_path, output),
        "stderr": network._artifact(process.stderr_path, output),
        "result": network._artifact(process.result_local, output) if process.result_local is not None else None,
    } for process in processes]


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        topology = network.load_topology(args.topology.expanduser().resolve())
        _validate_args(args, topology)
        output = network.prepare_output(args.output_dir)
    except network.ConfigurationError as exc:
        print(f"Mino hybrid configuration error: {exc}", file=sys.stderr)
        return 2

    started_utc = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    run_id = "hybrid-mino-" + hashlib.sha256(f"{time.time_ns()}:{os.getpid()}".encode()).hexdigest()[:16]
    errors: list[str] = []
    processes: list[ProcessRecord] = []
    edges: list[dict[str, Any]] = []
    boot_ids: dict[str, str] = {}
    shm_names: dict[str, str] = {}
    setups: list[tuple[network.RoleHost, str, Path]] = []
    sink: Optional[dict[str, Any]] = None
    same_host = False
    try:
        boot_ids = network.read_boot_ids(topology, min(10, args.deadline_seconds))
        same_host = len(set(boot_ids.values())) == 1
        shm_names = _shm_names(run_id, boot_ids)
        setup_errors, setups = _setup_segments(args, topology, boot_ids, shm_names, run_id)
        errors.extend(setup_errors)
        if not errors:
            processes, edges = create_processes(args, topology, boot_ids, shm_names, output, run_id)
            errors.extend(launch_and_wait(args, processes, run_id))
            validation_errors, sink = collect_and_validate(args, processes, boot_ids, run_id, same_host)
            errors.extend(validation_errors)
    except (OSError, subprocess.TimeoutExpired, network.ConfigurationError, RuntimeError) as exc:
        errors.append(f"hybrid orchestration failed: {exc}")
        terminate_processes(processes)
    finally:
        errors.extend(_cleanup_segments(args, setups, run_id, same_host))
        if not args.keep_remote_runtime:
            for process in processes:
                network._remote_cleanup(process.host, process.runtime_dir, 10)

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "outcome": "passed" if not errors else "failed",
        "errors": errors,
        "run_id": run_id,
        "backend": "mino_hybrid",
        "profile": args.profile,
        "clock_mode": "same-host" if same_host else "independent-hosts",
        "one_way_latency_valid": same_host and not errors,
        "host_identity_source": "Linux boot ID read through each role execution path",
        "config": {
            "messages": args.messages,
            "warmup_messages": args.warmup_messages,
            "publish_interval_us": args.publish_interval_us,
            "deadline_seconds": args.deadline_seconds,
            "port_base": args.port_base,
            "channel_capacity": args.channel_capacity,
            "schema_descriptor_relative": args.schema_descriptor_relative,
        },
        "boot_ids": boot_ids,
        "shm_segments": shm_names,
        "edges": edges,
        "started_utc": started_utc,
        "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "processes": _process_records(processes, output),
        "sink_metrics": sink,
    }
    temporary = output / ".manifest.tmp"
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, output / "manifest.json")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
