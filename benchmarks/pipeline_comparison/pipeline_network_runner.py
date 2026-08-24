#!/usr/bin/env python3
"""Run one six-process pipeline backend on one or more physical hosts.

Remote mode assumes the repository and requested binaries already exist at each
role's configured workdir. SSH is non-interactive. Cross-host runs use
independent-host clock semantics and never report one-way latency unless a
future runner adds an explicit PTP qualification contract.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import ipaddress
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from benchmarks.pipeline_comparison.pipeline_comparison_runner import (
    validate_worker_result,
)


TOPOLOGY_SCHEMA = "mino.pipeline_network_topology.v1"
MANIFEST_SCHEMA = "mino.pipeline_network_benchmark.manifest.v1"
WORKER_SCHEMA = "mino.pipeline_e2e_benchmark.worker.v1"
ROLES = ("perception", "prediction", "planning", "control", "guardian", "canbus")
PROFILES = ("small", "medium", "large")
BACKENDS = ("mino_tcp", "protobuf_zmq", "fastdds", "cyclonedds")
BACKEND_WORKER_NAMES = {
    "mino_tcp": "mino-tcp-canonical",
    "protobuf_zmq": "protobuf-zmq",
    "fastdds": "fastdds-idl",
    "cyclonedds": "cyclonedds-idl",
}
BACKEND_BINARIES = {
    "mino_tcp": "bazel-bin/benchmarks/pipeline_comparison/mino_tcp_pipeline",
    "protobuf_zmq": "bazel-bin/benchmarks/pipeline_comparison/protobuf_zmq_pipeline",
    "fastdds": "bazel-bin/benchmarks/pipeline_comparison/fastdds_pipeline",
    "cyclonedds": "bazel-bin/benchmarks/pipeline_comparison/cyclonedds_pipeline",
}
DEFAULT_DESCRIPTOR = (
    "bazel-bin/benchmarks/pipeline_comparison/mino_generated/"
    "autonomy_pipeline.descriptor"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
POLL_SECONDS = 0.05
CLEANUP_DEADLINE_SECONDS = 10.0


class ConfigurationError(ValueError):
    pass


class DeadlineExpired(RuntimeError):
    pass


def _deadline_timeout(deadline: float, cap: float, operation: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise DeadlineExpired(f"deadline expired {operation}")
    return min(cap, remaining)


def _has_control_character(value: str) -> bool:
    return any(unicodedata.category(character).startswith("C") for character in value)


@dataclass(frozen=True)
class RoleHost:
    role: str
    ssh_host: str
    data_address: str
    workdir: Path
    environment: Mapping[str, str]

    @property
    def local(self) -> bool:
        return self.ssh_host == "local"


@dataclass
class Worker:
    host: RoleHost
    runtime_dir: Path
    remote_result: Path
    local_result: Path
    stdout_path: Path
    stderr_path: Path
    command: list[str]
    launcher_command: list[str]
    process: Optional[subprocess.Popen[Any]] = None
    stdout_handle: Any = None
    stderr_handle: Any = None


def _bounded_int(name: str, minimum: int, maximum: int):
    def parse(value: str) -> int:
        try:
            result = int(value, 10)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"{name} requires an integer") from exc
        if result < minimum or result > maximum:
            raise argparse.ArgumentTypeError(
                f"{name} must be in [{minimum}, {maximum}]"
            )
        return result

    return parse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--backend", choices=BACKENDS, required=True)
    parser.add_argument("--profile", choices=PROFILES, default="small")
    parser.add_argument(
        "--messages", type=_bounded_int("--messages", 1, 1_000_000_000), default=1000
    )
    parser.add_argument(
        "--warmup-messages",
        type=_bounded_int("--warmup-messages", 0, 1_000_000_000),
        default=100,
    )
    parser.add_argument(
        "--publish-interval-us",
        type=_bounded_int("--publish-interval-us", 0, 60_000_000),
        default=0,
    )
    parser.add_argument(
        "--deadline-seconds",
        type=_bounded_int("--deadline-seconds", 1, 86_400),
        default=120,
    )
    parser.add_argument(
        "--domain-id", type=_bounded_int("--domain-id", 0, 232), default=73
    )
    parser.add_argument(
        "--history-depth",
        type=_bounded_int("--history-depth", 2, 4096),
        default=64,
    )
    parser.add_argument(
        "--port-base",
        type=_bounded_int("--port-base", 1, 65_526),
        default=24_000,
    )
    parser.add_argument(
        "--zmq-hwm", type=_bounded_int("--zmq-hwm", 1, 2_147_483_647), default=64
    )
    parser.add_argument(
        "--receive-batch-size",
        type=_bounded_int("--receive-batch-size", 1, 64),
        default=1,
        help="ordered TcpDriver receive batch size; 1 preserves benchmark fairness",
    )
    parser.add_argument(
        "--binary-relative",
        help="repository-relative worker binary; defaults by backend",
    )
    parser.add_argument(
        "--schema-descriptor-relative",
        default=DEFAULT_DESCRIPTOR,
        help="repository-relative minoc descriptor for mino_tcp",
    )
    parser.add_argument("--keep-remote-runtime", action="store_true")
    return parser


def _strict_keys(value: Any, expected: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be an object")
    actual = set(value)
    if actual != expected:
        raise ConfigurationError(
            f"{name} keys differ; missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )
    return value


def _safe_environment(value: Any, role: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"roles.{role}.environment must be an object")
    result: dict[str, str] = {}
    for key, item in value.items():
        if (
            not isinstance(key, str)
            or not key
            or not key.replace("_", "A").isalnum()
            or not isinstance(item, str)
            or "\0" in item
        ):
            raise ConfigurationError(
                f"roles.{role}.environment contains an unsafe entry"
            )
        result[key] = item
    return result


def load_topology(path: Path) -> dict[str, RoleHost]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigurationError(f"cannot load topology: {exc}") from exc
    top = _strict_keys(document, {"schema", "roles"}, "topology")
    if top["schema"] != TOPOLOGY_SCHEMA:
        raise ConfigurationError(f"topology.schema must be {TOPOLOGY_SCHEMA!r}")
    roles = top["roles"]
    if not isinstance(roles, dict) or set(roles) != set(ROLES):
        raise ConfigurationError("topology.roles must contain exactly the six roles")

    result: dict[str, RoleHost] = {}
    for role in ROLES:
        entry = roles[role]
        if not isinstance(entry, dict):
            raise ConfigurationError(f"roles.{role} must be an object")
        allowed = {"ssh_host", "data_address", "workdir", "environment"}
        extra = set(entry) - allowed
        missing = {"ssh_host", "data_address", "workdir"} - set(entry)
        if extra or missing:
            raise ConfigurationError(
                f"roles.{role} keys differ; missing={sorted(missing)}, "
                f"extra={sorted(extra)}"
            )
        ssh_host = entry["ssh_host"]
        data_address = entry["data_address"]
        workdir = entry["workdir"]
        if (
            not isinstance(ssh_host, str)
            or not ssh_host
            or ssh_host.startswith("-")
            or any(character.isspace() for character in ssh_host)
            or _has_control_character(ssh_host)
        ):
            raise ConfigurationError(f"roles.{role}.ssh_host is invalid")
        if not isinstance(data_address, str) or not data_address or "\0" in data_address:
            raise ConfigurationError(f"roles.{role}.data_address is invalid")
        if (
            not isinstance(workdir, str)
            or _has_control_character(workdir)
            or not Path(workdir).is_absolute()
        ):
            raise ConfigurationError(
                f"roles.{role}.workdir must be an absolute path without control characters"
            )
        if ssh_host == "local" and Path(workdir).resolve() != REPOSITORY_ROOT:
            raise ConfigurationError(
                f"roles.{role}.workdir must be {str(REPOSITORY_ROOT)!r} for local"
            )
        result[role] = RoleHost(
            role=role,
            ssh_host=ssh_host,
            data_address=data_address,
            workdir=Path(workdir),
            environment=_safe_environment(entry.get("environment"), role),
        )
    return result


def is_same_host(topology: Mapping[str, RoleHost]) -> bool:
    """Return the topology-level hint; measurement validity uses boot IDs."""
    identities = {host.ssh_host for host in topology.values()}
    return len(identities) == 1


def read_boot_ids(
    topology: Mapping[str, RoleHost], timeout: float
) -> dict[str, str]:
    """Read each role's Linux boot ID through the same execution path as workers."""
    result: dict[str, str] = {}
    deadline = time.monotonic() + timeout
    for role in ROLES:
        completed = _remote_run(
            topology[role],
            ["cat", "/proc/sys/kernel/random/boot_id"],
            _deadline_timeout(deadline, timeout, "qualifying remote clock hosts"),
        )
        boot_id = completed.stdout.strip() if completed.returncode == 0 else ""
        if not boot_id or any(character.isspace() for character in boot_id):
            detail = completed.stderr.strip() or "empty or invalid boot ID"
            raise ConfigurationError(f"cannot qualify clock host for {role}: {detail}")
        result[role] = boot_id
    return result


def prepare_output(path: Path) -> Path:
    try:
        result = path.expanduser().resolve()
        if result.exists():
            if not result.is_dir():
                raise ConfigurationError("output path exists and is not a directory")
            try:
                next(result.iterdir())
            except StopIteration:
                return result
            raise ConfigurationError("output directory must be empty")
        result.mkdir(parents=True)
        return result
    except OSError as exc:
        raise ConfigurationError(f"cannot prepare output directory: {exc}") from exc


def _write_json_atomic(path: Path, document: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(
            json.dumps(document, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise ConfigurationError(f"cannot write {path.name}: {exc}") from exc


def _ssh_prefix(host: RoleHost) -> list[str]:
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        host.ssh_host,
    ]


def _remote_run(host: RoleHost, arguments: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
    if host.local:
        return subprocess.run(
            arguments,
            cwd=host.workdir,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
    command = "cd " + shlex.quote(str(host.workdir)) + " && " + shlex.join(arguments)
    return subprocess.run(
        [*_ssh_prefix(host), command],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )


def _remote_mkdir(host: RoleHost, path: Path, timeout: float) -> None:
    completed = _remote_run(host, ["mkdir", "-p", "--", str(path)], timeout)
    if completed.returncode != 0:
        raise ConfigurationError(
            f"cannot create runtime for {host.role}: {completed.stderr.strip()}"
        )


def _remote_read(host: RoleHost, path: Path, timeout: float) -> Optional[str]:
    completed = _remote_run(host, ["cat", "--", str(path)], timeout)
    if completed.returncode == 0:
        return completed.stdout
    return None


def _remote_write_start(host: RoleHost, path: Path, run_id: str, timeout: float) -> None:
    if host.local:
        temporary = path.with_suffix(".tmp")
        temporary.write_text(run_id + "\n", encoding="utf-8")
        os.replace(temporary, path)
        return
    temporary = path.with_name(
        ".start." + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:12] + ".tmp"
    )
    command = (
        "set -eu; umask 077; printf '%s\\n' "
        + shlex.quote(run_id)
        + " > "
        + shlex.quote(str(temporary))
        + "; mv -f -- "
        + shlex.quote(str(temporary))
        + " "
        + shlex.quote(str(path))
    )
    completed = subprocess.run(
        [*_ssh_prefix(host), command],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"cannot create remote start for {host.role}: {completed.stderr.strip()}"
        )


def _remote_copy(host: RoleHost, source: Path, destination: Path, timeout: float) -> bool:
    if host.local:
        try:
            shutil.copy2(source, destination)
            return True
        except OSError:
            return False
    completed = subprocess.run(
        [
            "scp",
            "-q",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            f"{host.ssh_host}:{source}",
            str(destination),
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout,
    )
    return completed.returncode == 0


def _remote_cleanup(host: RoleHost, path: Path, timeout: float) -> None:
    completed = _remote_run(host, ["rm", "-rf", "--", str(path)], timeout)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(f"cannot remove runtime for {host.role}: {detail}")


def _sha256(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _artifact(path: Path, output: Path) -> dict[str, Optional[str]]:
    return {
        "path": path.resolve().relative_to(output).as_posix(),
        "sha256": _sha256(path),
    }


def _common_arguments(
    args: argparse.Namespace,
    role: str,
    run_id: str,
    runtime_dir: Path,
    output: Path,
    same_host: bool,
) -> list[str]:
    return [
        "--role",
        role,
        "--profile",
        args.profile,
        "--messages",
        str(args.messages),
        "--warmup-messages",
        str(args.warmup_messages),
        "--publish-interval-us",
        str(args.publish_interval_us),
        "--deadline-seconds",
        str(args.deadline_seconds),
        "--clock-mode",
        "same-host" if same_host else "independent-hosts",
        "--run-id",
        run_id,
        "--runtime-dir",
        str(runtime_dir),
        "--output",
        str(output),
    ]


def _backend_arguments(
    args: argparse.Namespace,
    topology: Mapping[str, RoleHost],
    role: str,
    same_host: bool,
    boot_ids: Optional[Mapping[str, str]] = None,
) -> list[str]:
    if args.backend in ("fastdds", "cyclonedds"):
        marker = ["--cyclonedds-backend"] if args.backend == "cyclonedds" else []
        return [
            "--domain-id",
            str(args.domain_id),
            "--history-depth",
            str(args.history_depth),
            *marker,
        ]
    role_index = ROLES.index(role)
    downstream_index = min(role_index + 1, len(ROLES) - 1)
    peer_address = topology[ROLES[downstream_index]].data_address
    if args.backend == "protobuf_zmq":
        identity = boot_ids or {name: topology[name].ssh_host for name in ROLES}
        upstream_index = max(role_index - 1, 0)
        input_local = role_index == 0 or identity[ROLES[upstream_index]] == identity[role]
        output_local = role_index == len(ROLES) - 1 or identity[role] == identity[ROLES[downstream_index]]
        return [
            "--input-transport", "ipc" if input_local else "tcp",
            "--output-transport", "ipc" if output_local else "tcp",
            "--listen-address", "127.0.0.1" if input_local and output_local else "0.0.0.0",
            "--peer-address", peer_address,
            "--upstream-address", topology[ROLES[upstream_index]].data_address,
            "--port-base", str(args.port_base),
            "--hwm", str(args.zmq_hwm),
        ]
    return [
        "--listen-address",
        topology[role].data_address if same_host else "0.0.0.0",
        "--peer-address",
        peer_address,
        "--port-base",
        str(args.port_base),
        "--schema-descriptor",
        str(topology[role].workdir / args.schema_descriptor_relative),
        "--receive-batch-size",
        str(args.receive_batch_size),
    ]


def create_workers(
    args: argparse.Namespace,
    topology: Mapping[str, RoleHost],
    output: Path,
    run_id: str,
    same_host: bool,
    boot_ids: Optional[Mapping[str, str]] = None,
) -> list[Worker]:
    if args.backend in ("mino_tcp", "protobuf_zmq"):
        for role in ROLES:
            try:
                ipaddress.IPv4Address(topology[role].data_address)
            except ipaddress.AddressValueError as exc:
                raise ConfigurationError(
                    f"roles.{role}.data_address must be a numeric IPv4 address for {args.backend}"
                ) from exc
    workers: list[Worker] = []
    binary_relative = args.binary_relative or BACKEND_BINARIES[args.backend]
    results = output / "results"
    logs = output / "logs"
    results.mkdir()
    logs.mkdir()
    token = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    for role in ROLES:
        host = topology[role]
        if args.backend == "protobuf_zmq":
            identity = (boot_ids or {}).get(role, host.ssh_host)
            host_token = hashlib.sha256(identity.encode()).hexdigest()[:12]
            runtime = Path("/tmp") / f"mino-network-{token}-host-{host_token}"
            remote_result = runtime / f"{role}.json"
        else:
            runtime = Path("/tmp") / f"mino-network-{token}-{role}"
            remote_result = runtime / "result.json"
        local_result = results / f"{role}.json"
        command = [
            str(host.workdir / binary_relative),
            *_common_arguments(
                args, role, run_id, runtime, remote_result, same_host
            ),
            *_backend_arguments(args, topology, role, same_host, boot_ids),
        ]
        environment = ["env", *[f"{key}={value}" for key, value in sorted(host.environment.items())]]
        remote_command = [*environment, *command]
        launcher = remote_command
        if not host.local:
            pid_path = runtime / f"worker-{role}.pid"
            remote_script = (
                "umask 077; printf '%s\\n' \"$$\" > "
                + shlex.quote(str(pid_path))
                + "; exec "
                + shlex.join(remote_command)
            )
            launcher = [
                *_ssh_prefix(host),
                "cd "
                + shlex.quote(str(host.workdir))
                + " && exec setsid sh -c "
                + shlex.quote(remote_script),
            ]
        workers.append(
            Worker(
                host=host,
                runtime_dir=runtime,
                remote_result=remote_result,
                local_result=local_result,
                stdout_path=logs / f"{role}.stdout.log",
                stderr_path=logs / f"{role}.stderr.log",
                command=command,
                launcher_command=launcher,
            )
        )
    return workers


def _remote_process_group(worker: Worker, deadline: float) -> Optional[int]:
    if worker.host.local:
        return None
    try:
        content = _remote_read(
            worker.host,
            worker.runtime_dir / f"worker-{worker.host.role}.pid",
            _deadline_timeout(deadline, 2, "reading a remote worker pid"),
        )
    except (OSError, subprocess.TimeoutExpired, DeadlineExpired):
        return None
    if content is None:
        return None
    text = content.strip()
    if not text.isascii() or not text.isdecimal():
        return None
    pid = int(text, 10)
    return pid if 1 < pid <= 2_147_483_647 else None


def _signal_remote_process_group(
    worker: Worker, pid: int, name: str, deadline: float
) -> None:
    try:
        _remote_run(
            worker.host,
            ["/bin/kill", f"-{name}", "--", f"-{pid}"],
            _deadline_timeout(deadline, 3, f"sending remote {name}"),
        )
    except (OSError, subprocess.TimeoutExpired, DeadlineExpired):
        pass


def terminate_workers(workers: Sequence[Worker]) -> None:
    deadline = time.monotonic() + CLEANUP_DEADLINE_SECONDS
    active = [
        worker.process
        for worker in workers
        if worker.process is not None and worker.process.poll() is None
    ]
    for process in active:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    remote_groups: list[tuple[Worker, int]] = []
    for worker in workers:
        pid = _remote_process_group(worker, deadline)
        if pid is not None:
            remote_groups.append((worker, pid))
    for worker, pid in remote_groups:
        _signal_remote_process_group(worker, pid, "TERM", deadline)

    grace_deadline = min(deadline, time.monotonic() + 1.0)
    while any(process.poll() is None for process in active):
        remaining = grace_deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.02, remaining))
    for worker, pid in remote_groups:
        _signal_remote_process_group(worker, pid, "KILL", deadline)
    for process in active:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    for process in active:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            break


def launch_and_wait(
    args: argparse.Namespace,
    workers: list[Worker],
    run_id: str,
) -> list[str]:
    errors: list[str] = []
    completed = False
    deadline = time.monotonic() + args.deadline_seconds
    try:
        for worker in workers:
            _remote_mkdir(
                worker.host,
                worker.runtime_dir,
                _deadline_timeout(deadline, 10, "setting up remote runtimes"),
            )
            _deadline_timeout(deadline, 10, "launching remote workers")
            worker.stdout_handle = worker.stdout_path.open("wb")
            worker.stderr_handle = worker.stderr_path.open("wb")
            worker.process = subprocess.Popen(
                worker.launcher_command,
                cwd=worker.host.workdir if worker.host.local else None,
                stdout=worker.stdout_handle,
                stderr=worker.stderr_handle,
                start_new_session=True,
            )

        pending = {worker.host.role: worker for worker in workers}
        while pending:
            for role, worker in tuple(pending.items()):
                if worker.process is not None and worker.process.poll() is not None:
                    errors.append(
                        f"{role} exited with {worker.process.returncode} before readiness"
                    )
                    return errors
                ready = worker.runtime_dir / (
                    f"{BACKEND_WORKER_NAMES[args.backend]}-{role}.ready"
                )
                content = _remote_read(
                    worker.host,
                    ready,
                    _deadline_timeout(
                        deadline, 5, "waiting for remote readiness"
                    ),
                )
                if content == run_id + "\n":
                    del pending[role]
                elif content is not None:
                    errors.append(f"{role} readiness run-id mismatch")
                    return errors
            if pending:
                time.sleep(
                    _deadline_timeout(
                        deadline, POLL_SECONDS, "waiting for remote readiness"
                    )
                )

        # Start consumers first. Per-role runtime directories prevent a source
        # start from racing a downstream process on the same physical host.
        for worker in reversed(workers[1:]):
            _remote_write_start(
                worker.host,
                worker.runtime_dir / "start",
                run_id,
                _deadline_timeout(deadline, 10, "starting remote workers"),
            )
        _remote_write_start(
            workers[0].host,
            workers[0].runtime_dir / "start",
            run_id,
            _deadline_timeout(deadline, 10, "starting remote workers"),
        )

        pending_workers = list(workers)
        while pending_workers:
            for worker in tuple(pending_workers):
                assert worker.process is not None
                code = worker.process.poll()
                if code is None:
                    continue
                pending_workers.remove(worker)
                if code != 0:
                    errors.append(f"{worker.host.role} exited with {code}")
            if errors:
                return errors
            if pending_workers:
                time.sleep(
                    _deadline_timeout(
                        deadline, POLL_SECONDS, "waiting for remote workers"
                    )
                )
        completed = True
    except (OSError, subprocess.TimeoutExpired, ConfigurationError, RuntimeError) as exc:
        errors.append(f"orchestration failed: {exc}")
    finally:
        # For SSH workers this also cleans up a detached remote group if the
        # local SSH client exited before the benchmark process did.
        if errors or not completed:
            terminate_workers(workers)
        for worker in workers:
            if worker.stdout_handle is not None:
                worker.stdout_handle.close()
            if worker.stderr_handle is not None:
                worker.stderr_handle.close()
    return errors


def collect_results(
    args: argparse.Namespace,
    workers: Sequence[Worker],
) -> list[str]:
    errors: list[str] = []
    collection_deadline = time.monotonic() + args.deadline_seconds
    for worker in workers:
        try:
            copied = _remote_copy(
                worker.host,
                worker.remote_result,
                worker.local_result,
                _deadline_timeout(
                    collection_deadline, 30, "collecting remote result JSON"
                ),
            )
        except (OSError, subprocess.TimeoutExpired, RuntimeError):
            copied = False
        if not copied:
            errors.append(f"{worker.host.role}: could not collect result JSON")
    if not args.keep_remote_runtime:
        cleanup_deadline = time.monotonic() + CLEANUP_DEADLINE_SECONDS
        cleaned: set[tuple[str, Path]] = set()
        for worker in workers:
            key = (worker.host.ssh_host, worker.runtime_dir)
            if key in cleaned:
                continue
            cleaned.add(key)
            try:
                _remote_cleanup(
                    worker.host,
                    worker.runtime_dir,
                    _deadline_timeout(
                        cleanup_deadline, 10, "cleaning remote runtimes"
                    ),
                )
            except (OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
                errors.append(f"{worker.host.role}: remote cleanup failed: {exc}")
    return errors


def validate_result(
    document: Any,
    *,
    args: argparse.Namespace,
    worker: Worker,
    run_id: str,
    same_host: bool,
    expected_boot_id: str,
) -> dict[str, Any]:
    validated = validate_worker_result(
        document,
        expected_backend=BACKEND_WORKER_NAMES[args.backend],
        expected_role=worker.host.role,
        expected_profile=args.profile,
        expected_run_id=run_id,
        expected_messages=args.messages,
        expected_warmup_messages=args.warmup_messages,
        expected_publish_interval_us=args.publish_interval_us,
        expected_deadline_seconds=args.deadline_seconds,
        expected_clock_mode="same-host" if same_host else "independent-hosts",
        expected_runtime_directory=worker.runtime_dir,
        expected_output=worker.remote_result,
        expect_sink_latency=same_host,
    )
    if validated["clock"]["boot_id"] != expected_boot_id:
        raise ValueError("clock.boot_id differs from the preflight host identity")
    return validated


def validate_results(
    args: argparse.Namespace,
    workers: Sequence[Worker],
    run_id: str,
    same_host: bool,
    boot_ids: Mapping[str, str],
) -> tuple[list[str], Optional[dict[str, Any]]]:
    errors: list[str] = []
    clocks: list[tuple[str, int, str]] = []
    sink: Optional[dict[str, Any]] = None
    for worker in workers:
        if not worker.local_result.is_file():
            continue
        try:
            document = json.loads(worker.local_result.read_text(encoding="utf-8"))
            validated = validate_result(
                document,
                args=args,
                worker=worker,
                run_id=run_id,
                same_host=same_host,
                expected_boot_id=boot_ids[worker.host.role],
            )
            clock = validated["clock"]
            clocks.append((clock["name"], clock["resolution_ns"], clock["boot_id"]))
            if worker.host.role == "canbus":
                sink = {
                    "latency_ns": validated["latency_ns"] if same_host else None,
                    "elapsed_ns": validated["elapsed_ns"],
                    "throughput_messages_per_second": validated[
                        "throughput_messages_per_second"
                    ],
                    "payload_bytes": validated["payload_bytes"],
                    "encoded_bytes": validated["encoded_bytes"],
                    "backend_details": validated["backend_details"],
                }
        except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            errors.append(f"{worker.host.role}: invalid result: {exc}")
    if same_host and clocks and any(clock != clocks[0] for clock in clocks[1:]):
        errors.append("same-host worker clock name/resolution/boot_id values differ")
    if sink is None:
        errors.append("validated canbus result is missing")
    return errors, sink


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        topology = load_topology(args.topology.expanduser().resolve())
        output = prepare_output(args.output_dir)
    except ConfigurationError as exc:
        print(f"network pipeline configuration error: {exc}", file=sys.stderr)
        return 2

    started_utc = (
        dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    )
    run_id = (
        f"network-{args.backend}-{args.profile}-"
        + hashlib.sha256(
            f"{time.time_ns()}:{os.getpid()}".encode()
        ).hexdigest()[:16]
    )
    errors: list[str] = []
    workers: list[Worker] = []
    boot_ids: dict[str, str] = {}
    same_host = False
    sink: Optional[dict[str, Any]] = None
    try:
        boot_ids = read_boot_ids(topology, min(10, args.deadline_seconds))
        same_host = len(set(boot_ids.values())) == 1
        workers = create_workers(
            args, topology, output, run_id, same_host, boot_ids
        )
        errors.extend(launch_and_wait(args, workers, run_id))
        errors.extend(collect_results(args, workers))
        validation_errors, sink = validate_results(
            args, workers, run_id, same_host, boot_ids
        )
        errors.extend(validation_errors)
    except (OSError, subprocess.TimeoutExpired, ConfigurationError, RuntimeError) as exc:
        errors.append(f"orchestration failed: {exc}")
        terminate_workers(workers)

    try:
        records = []
        for worker in workers:
            records.append(
                {
                    "role": worker.host.role,
                    "ssh_host": worker.host.ssh_host,
                    "data_address": worker.host.data_address,
                    "boot_id": boot_ids.get(worker.host.role),
                    "workdir": str(worker.host.workdir),
                    "command": worker.command,
                    "launcher_exit_code": (
                        worker.process.poll() if worker.process is not None else None
                    ),
                    "stdout": _artifact(worker.stdout_path, output),
                    "stderr": _artifact(worker.stderr_path, output),
                    "result": _artifact(worker.local_result, output),
                }
            )
        manifest = {
            "schema": MANIFEST_SCHEMA,
            "outcome": "passed" if not errors else "failed",
            "errors": errors,
            "run_id": run_id,
            "backend": args.backend,
            "profile": args.profile,
            "clock_mode": "same-host" if same_host else "independent-hosts",
            "one_way_latency_valid": same_host and not errors,
            "host_identity_source": "Linux boot ID read through each role execution path",
            "config": {
                "messages": args.messages,
                "warmup_messages": args.warmup_messages,
                "publish_interval_us": args.publish_interval_us,
                "deadline_seconds": args.deadline_seconds,
                "domain_id": args.domain_id,
                "history_depth": args.history_depth,
                "port_base": args.port_base,
                "zmq_hwm": args.zmq_hwm,
                "receive_batch_size": args.receive_batch_size,
            },
            "started_utc": started_utc,
            "finished_utc": dt.datetime.now(dt.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "workers": records,
            "sink_metrics": sink,
        }
        _write_json_atomic(output / "manifest.json", manifest)
    except (OSError, ConfigurationError) as exc:
        print(f"network pipeline output error: {exc}", file=sys.stderr)
        return 1
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
