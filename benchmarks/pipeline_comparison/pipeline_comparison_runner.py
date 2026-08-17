#!/usr/bin/env python3
"""Qualification runner for the same-host pipeline backend comparison.

The measured values are same-host one-way latencies.  They are not valid
cross-host one-way measurements.  Mino uses the benchmark-static manifest
boundary described by this benchmark; Fast DDS uses its DEFAULT builtin UDPv4
plus intrahost SHM transport with automatic data sharing; ZeroMQ uses IPC.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import platform
import random
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


MANIFEST_SCHEMA = "mino.pipeline_e2e_comparison.manifest.v1"
WORKER_SCHEMA = "mino.pipeline_e2e_benchmark.worker.v1"
ROLES = ("perception", "prediction", "planning", "control", "guardian", "canbus")
PROFILES = ("small", "medium", "large")
DEFAULT_SEED = 20260308
READINESS_POLL_SECONDS = 0.01
TERMINATION_GRACE_SECONDS = 0.5

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIRECTORY = REPOSITORY_ROOT / "benchmarks" / "pipeline_comparison"


class RunnerConfigurationError(ValueError):
    """Raised before any qualification artifacts are created."""


class RunFailure(RuntimeError):
    """Raised for a bounded worker orchestration failure."""


@dataclass(frozen=True)
class BackendSpec:
    name: str
    worker_backend: str
    default_binary: Path
    dependency_labels: Tuple[str, ...]


BACKEND_SPECS: Mapping[str, BackendSpec] = {
    "fastdds_pipeline": BackendSpec(
        name="fastdds_pipeline",
        worker_backend="fastdds-idl",
        default_binary=REPOSITORY_ROOT
        / "bazel-bin/benchmarks/pipeline_comparison/fastdds_pipeline",
        dependency_labels=(
            "//benchmarks/pipeline_comparison:fastdds_pipeline",
            "//benchmarks/pipeline_comparison:autonomy_pipeline_fastdds_type",
            "//benchmarks/pipeline_comparison:pipeline_common",
            "@fastdds//:fastdds",
            "@fastcdr//:fastcdr",
        ),
    ),
    "cyclonedds_pipeline": BackendSpec(
        name="cyclonedds_pipeline",
        worker_backend="cyclonedds-idl",
        default_binary=REPOSITORY_ROOT
        / "bazel-bin/benchmarks/pipeline_comparison/cyclonedds_pipeline",
        dependency_labels=(
            "//benchmarks/pipeline_comparison:cyclonedds_pipeline",
            "//benchmarks/pipeline_comparison:autonomy_pipeline_cyclonedds_type",
            "//benchmarks/pipeline_comparison:pipeline_common",
            "@cyclonedds//:ddsc",
        ),
    ),
    "protobuf_zmq_pipeline": BackendSpec(
        name="protobuf_zmq_pipeline",
        worker_backend="protobuf-zmq",
        default_binary=REPOSITORY_ROOT
        / "bazel-bin/benchmarks/pipeline_comparison/protobuf_zmq_pipeline",
        dependency_labels=(
            "//benchmarks/pipeline_comparison:protobuf_zmq_pipeline",
            "//benchmarks/pipeline_comparison:autonomy_pipeline_cc_proto",
            "//benchmarks/pipeline_comparison:pipeline_common",
            "@libzmq",
            "@protobuf//:protobuf",
        ),
    ),
    "mino_shm_pipeline": BackendSpec(
        name="mino_shm_pipeline",
        worker_backend="mino-shm",
        default_binary=REPOSITORY_ROOT
        / "bazel-bin/benchmarks/pipeline_comparison/mino_shm_pipeline",
        dependency_labels=(
            "//benchmarks/pipeline_comparison:mino_shm_pipeline",
            "//benchmarks/pipeline_comparison:pipeline_common",
            "//mino/platform:shared_memory",
            "//mino/runtime:publisher",
            "//mino/runtime:subscriber",
            "//mino/shm/allocator:central_slab",
            "//mino/shm/channel:spsc",
        ),
    ),
}
BACKEND_NAMES = tuple(BACKEND_SPECS)

SCHEMA_PATHS = (
    BENCHMARK_DIRECTORY / "autonomy_pipeline.idl",
    BENCHMARK_DIRECTORY / "autonomy_pipeline.proto",
    BENCHMARK_DIRECTORY / "autonomy_pipeline.mino",
)
BUILD_INPUT_PATHS = (
    REPOSITORY_ROOT / ".bazelrc",
    REPOSITORY_ROOT / "MODULE.bazel",
    REPOSITORY_ROOT / "MODULE.bazel.lock",
    BENCHMARK_DIRECTORY / "BUILD.bazel",
    BENCHMARK_DIRECTORY / "generated/AutonomyPipelineFrame.hpp",
    BENCHMARK_DIRECTORY / "generated/AutonomyPipelineFrameCdrAux.hpp",
    BENCHMARK_DIRECTORY / "generated/AutonomyPipelineFrameCdrAux.ipp",
    BENCHMARK_DIRECTORY / "generated/AutonomyPipelineFramePubSubTypes.hpp",
    BENCHMARK_DIRECTORY / "generated/AutonomyPipelineFramePubSubTypes.cxx",
)


METHODOLOGY = {
    "topology": list(ROLES),
    "measurement": (
        "Same-host one-way end-to-end latency from source origin timestamp to "
        "CANBus validation completion; warmup and control barriers are excluded."
    ),
    "same_host_one_way_limit": (
        "Results are valid only for processes on one host and one boot sharing "
        "the Linux monotonic clock; they must not be interpreted as cross-host "
        "one-way latency."
    ),
    "serialization": "Backends run serially; the four backends never overlap.",
    "buffering_semantics": (
        "The nominal value 64 is not a strict equal window: Mino SPSC capacity "
        "is exact, DDS has bounded reader/writer histories, and ZeroMQ HWM is a "
        "per-pipe approximation. Saturation latency is native queueing behavior."
    ),
    "paced_schedule": (
        "A paced source fails if it falls more than one complete publish interval "
        "behind its absolute schedule; catch-up bursts are not accepted as paced latency."
    ),
    "mino_scope_boundary": (
        "Mino exercises production allocator/channel/typed-runtime hot paths over "
        "a benchmark-static shared manifest; it does not qualify cross-process "
        "Bus discovery or supervisor lifecycle."
    ),
    "fastdds_transport": (
        "Fast DDS DEFAULT builtin UDPv4 plus intrahost SHM, not SHM-only; "
        "default builtin UDP discovery and data sharing automatic."
    ),
    "cyclonedds_transport": (
        "Cyclone DDS default UDP transport and SPDP discovery, optionally "
        "configured by CYCLONEDDS_URI; the BCR build has no PSMX plugin."
    ),
    "protobuf_zmq_transport": "Native ZeroMQ IPC push/pull sockets.",
}


def _bounded_int(name: str, minimum: int, maximum: int):
    def parse(value: str) -> int:
        try:
            parsed = int(value, 10)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"{name} requires an integer") from exc
        if parsed < minimum or parsed > maximum:
            raise argparse.ArgumentTypeError(
                f"{name} must be in [{minimum}, {maximum}]"
            )
        return parsed

    return parse


def _bounded_ratio(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--warmup-ratio requires a number") from exc
    if not math.isfinite(parsed) or parsed < 0.0 or parsed > 1.0:
        raise argparse.ArgumentTypeError("--warmup-ratio must be in [0, 1]")
    return parsed


def _profiles(value: str) -> Tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(","))
    if not values or any(not part for part in values):
        raise argparse.ArgumentTypeError("--profiles requires a non-empty comma list")
    invalid = [part for part in values if part not in PROFILES]
    if invalid:
        raise argparse.ArgumentTypeError(
            "--profiles entries must be small, medium, or large"
        )
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("--profiles may not contain duplicates")
    return values


def _backends(value: str) -> Tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(","))
    if not values or any(not part for part in values):
        raise argparse.ArgumentTypeError("--backends requires a non-empty comma list")
    invalid = [part for part in values if part not in BACKEND_SPECS]
    if invalid:
        raise argparse.ArgumentTypeError(
            "--backends contains unknown entries: " + ", ".join(invalid)
        )
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("--backends may not contain duplicates")
    return values


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--backends", type=_backends, default=BACKEND_NAMES)
    parser.add_argument("--rounds", type=_bounded_int("--rounds", 1, 20), default=3)
    parser.add_argument("--profiles", type=_profiles, default=PROFILES)
    parser.add_argument(
        "--small-messages",
        type=_bounded_int("--small-messages", 1, 1_000_000_000),
        default=10_000,
    )
    parser.add_argument(
        "--medium-messages",
        type=_bounded_int("--medium-messages", 1, 1_000_000_000),
        default=2_000,
    )
    parser.add_argument(
        "--large-messages",
        type=_bounded_int("--large-messages", 1, 1_000_000_000),
        default=200,
    )
    parser.add_argument("--warmup-ratio", type=_bounded_ratio, default=0.1)
    parser.add_argument(
        "--small-publish-interval-us",
        type=_bounded_int("--small-publish-interval-us", 0, 60_000_000),
        default=0,
    )
    parser.add_argument(
        "--medium-publish-interval-us",
        type=_bounded_int("--medium-publish-interval-us", 0, 60_000_000),
        default=0,
    )
    parser.add_argument(
        "--large-publish-interval-us",
        type=_bounded_int("--large-publish-interval-us", 0, 60_000_000),
        default=0,
    )
    parser.add_argument(
        "--deadline-seconds",
        type=_bounded_int("--deadline-seconds", 1, 86_400),
        default=120,
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--domain-id-base",
        type=_bounded_int("--domain-id-base", 0, 232),
        default=73,
    )
    parser.add_argument(
        "--history-depth",
        type=_bounded_int("--history-depth", 2, 4096),
        default=64,
    )
    parser.add_argument(
        "--zmq-hwm", type=_bounded_int("--zmq-hwm", 1, 2_147_483_647), default=64
    )
    parser.add_argument(
        "--channel-capacity",
        type=_bounded_int("--channel-capacity", 2, 4096),
        default=64,
    )
    parser.add_argument(
        "--fastdds-binary", type=Path, default=BACKEND_SPECS["fastdds_pipeline"].default_binary
    )
    parser.add_argument(
        "--cyclonedds-binary",
        type=Path,
        default=BACKEND_SPECS["cyclonedds_pipeline"].default_binary,
    )
    parser.add_argument(
        "--protobuf-zmq-binary",
        type=Path,
        default=BACKEND_SPECS["protobuf_zmq_pipeline"].default_binary,
    )
    parser.add_argument(
        "--mino-shm-binary",
        type=Path,
        default=BACKEND_SPECS["mino_shm_pipeline"].default_binary,
    )
    parser.add_argument("--commit")
    parser.add_argument("--fail-fast", action="store_true")
    return parser


def prepare_output_directory(path: Path) -> Path:
    output = path.expanduser().resolve()
    if output.exists():
        if not output.is_dir():
            raise RunnerConfigurationError(f"output path is not a directory: {output}")
        try:
            next(output.iterdir())
        except StopIteration:
            return output
        raise RunnerConfigurationError(f"output directory is not empty: {output}")
    output.mkdir(parents=True)
    return output


def backend_order(
    seed: int,
    profile: str,
    round_index: int,
    backends: Sequence[str] = BACKEND_NAMES,
) -> Tuple[str, ...]:
    """Return a deterministic shuffled base order, rotated once per round."""
    if profile not in PROFILES:
        raise ValueError(f"unknown profile: {profile}")
    if round_index < 0:
        raise ValueError("round_index must be non-negative")
    if not backends or any(backend not in BACKEND_SPECS for backend in backends):
        raise ValueError("backends must contain known backend names")
    if len(set(backends)) != len(backends):
        raise ValueError("backends may not contain duplicates")
    stable_seed = hashlib.sha256(f"{seed}:{profile}".encode("utf-8")).digest()
    shuffled = list(backends)
    random.Random(int.from_bytes(stable_seed[:8], "big")).shuffle(shuffled)
    offset = round_index % len(shuffled)
    return tuple(shuffled[offset:] + shuffled[:offset])


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _artifact(path: Path, output_directory: Path) -> Dict[str, Optional[str]]:
    relative = path.resolve().relative_to(output_directory).as_posix()
    return {"path": relative, "sha256": _sha256(path) if path.is_file() else None}


def _atomic_write_json(path: Path, document: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_create_start(runtime_directory: Path, run_id: str) -> None:
    temporary = runtime_directory / f".start.{secrets.token_hex(6)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(run_id + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, runtime_directory / "start")
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _make_runtime_directory(run_id: str) -> Path:
    for attempt in range(100):
        token = hashlib.sha256(
            f"{run_id}:{attempt}:{secrets.token_hex(4)}".encode("utf-8")
        ).hexdigest()[:14]
        path = Path("/tmp") / f"mino-pipeline-{token}"
        try:
            path.mkdir(mode=0o700)
            return path
        except FileExistsError:
            continue
    raise RunFailure("could not allocate a short unique /tmp runtime directory")


def _terminate_process_groups(
    processes: Iterable[subprocess.Popen[Any]],
    grace_seconds: Optional[float] = None,
) -> None:
    if grace_seconds is None:
        grace_seconds = TERMINATION_GRACE_SECONDS
    active = [process for process in processes if process.poll() is None]
    for process in active:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    grace_deadline = time.monotonic() + grace_seconds
    while any(process.poll() is None for process in active):
        if time.monotonic() >= grace_deadline:
            break
        time.sleep(min(READINESS_POLL_SECONDS, max(0.0, grace_deadline - time.monotonic())))
    for process in active:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    for process in active:
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            pass


def wait_for_readiness(
    runtime_directory: Path,
    worker_backend: str,
    run_id: str,
    processes: Mapping[str, subprocess.Popen[Any]],
    deadline: float,
) -> None:
    pending = set(ROLES)
    while pending:
        for role in tuple(pending):
            ready_path = runtime_directory / f"{worker_backend}-{role}.ready"
            if ready_path.is_file():
                try:
                    content = ready_path.read_text(encoding="utf-8")
                except (OSError, UnicodeError) as exc:
                    raise RunFailure(f"cannot read readiness file {ready_path.name}: {exc}") from exc
                if content != run_id + "\n":
                    raise RunFailure(
                        f"readiness file {ready_path.name} contains a mismatched run-id"
                    )
                pending.remove(role)
                continue
            return_code = processes[role].poll()
            if return_code is not None:
                raise RunFailure(
                    f"{role} exited with {return_code} before publishing readiness"
                )
        if not pending:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RunFailure(
                "deadline expired waiting for readiness: " + ", ".join(sorted(pending))
            )
        time.sleep(min(READINESS_POLL_SECONDS, remaining))


def _wait_for_workers(
    processes: Mapping[str, subprocess.Popen[Any]], deadline: float
) -> None:
    pending = set(ROLES)
    while pending:
        for role in tuple(pending):
            return_code = processes[role].poll()
            if return_code is None:
                continue
            pending.remove(role)
            if return_code != 0:
                raise RunFailure(f"{role} exited with {return_code}")
        if not pending:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RunFailure(
                "deadline expired waiting for workers: " + ", ".join(sorted(pending))
            )
        time.sleep(min(READINESS_POLL_SECONDS, remaining))


def _common_arguments(
    profile: str,
    messages: int,
    warmup_messages: int,
    publish_interval_us: int,
    deadline_seconds: int,
    run_id: str,
    runtime_directory: Path,
    role: str,
    output: Path,
) -> List[str]:
    return [
        "--role",
        role,
        "--profile",
        profile,
        "--messages",
        str(messages),
        "--warmup-messages",
        str(warmup_messages),
        "--publish-interval-us",
        str(publish_interval_us),
        "--deadline-seconds",
        str(deadline_seconds),
        "--clock-mode",
        "same-host",
        "--run-id",
        run_id,
        "--runtime-dir",
        str(runtime_directory),
        "--output",
        str(output),
    ]


def _backend_arguments(
    backend_name: str,
    domain_id: int,
    history_depth: int,
    zmq_hwm: int,
    channel_capacity: int,
    run_id: str,
    operation: Optional[str] = None,
) -> List[str]:
    if backend_name == "fastdds_pipeline":
        return ["--domain-id", str(domain_id), "--history-depth", str(history_depth)]
    if backend_name == "cyclonedds_pipeline":
        return [
            "--domain-id",
            str(domain_id),
            "--history-depth",
            str(history_depth),
            "--cyclonedds-backend",
        ]
    if backend_name == "protobuf_zmq_pipeline":
        return ["--hwm", str(zmq_hwm)]
    if backend_name == "mino_shm_pipeline":
        arguments = [
            "--operation",
            operation or "worker",
            "--shm-name",
            "/mino-pipeline-" + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:20],
            "--channel-capacity",
            str(channel_capacity),
        ]
        return arguments
    raise ValueError(f"unknown backend: {backend_name}")


def _run_control_command(
    command: Sequence[str],
    stdout_path: Path,
    stderr_path: Path,
    result_path: Path,
    output_directory: Path,
    timeout_seconds: int,
) -> Tuple[Dict[str, Any], Optional[str]]:
    record: Dict[str, Any] = {"command": list(command), "exit_code": None}
    error: Optional[str] = None
    process: Optional[subprocess.Popen[Any]] = None
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        try:
            process = subprocess.Popen(
                command, stdout=stdout, stderr=stderr, start_new_session=True
            )
            try:
                process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                error = f"control command timed out after {timeout_seconds}s"
                _terminate_process_groups([process])
            record["exit_code"] = process.poll()
            if error is None and record["exit_code"] != 0:
                error = f"control command exited with {record['exit_code']}"
        except OSError as exc:
            error = f"cannot launch control command: {exc}"
    record["stdout"] = _artifact(stdout_path, output_directory)
    record["stderr"] = _artifact(stderr_path, output_directory)
    record["result"] = _artifact(result_path, output_directory)
    return record, error


def _strict_object(value: Any, name: str, keys: Iterable[str]) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    expected = set(keys)
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ValueError(f"{name} keys differ; missing={missing}, extra={extra}")
    return value


def _strict_integer(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _strict_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be a finite number")
    return parsed


def validate_worker_result(
    document: Any,
    *,
    expected_backend: str,
    expected_role: str,
    expected_profile: str,
    expected_run_id: str,
    expected_messages: int,
    expected_warmup_messages: int,
    expected_publish_interval_us: int = 0,
    expected_deadline_seconds: Optional[int] = None,
    expected_clock_mode: str = "same-host",
    expected_runtime_directory: Optional[Path] = None,
    expected_output: Optional[Path] = None,
    expect_sink_latency: bool = True,
) -> Dict[str, Any]:
    """Strictly validate one common WriteSinkResult JSON document."""
    top = _strict_object(
        document,
        "worker result",
        (
            "schema",
            "backend",
            "role",
            "profile",
            "configuration",
            "clock",
            "counts",
            "latency_ns",
            "elapsed_ns",
            "throughput_messages_per_second",
            "payload_bytes",
            "encoded_bytes",
            "outcome",
            "error",
            "backend_details",
        ),
    )
    expected_values = {
        "schema": WORKER_SCHEMA,
        "backend": expected_backend,
        "role": expected_role,
        "profile": expected_profile,
        "outcome": "success",
        "error": "",
    }
    for key, expected in expected_values.items():
        if top[key] != expected:
            raise ValueError(f"{key} must be {expected!r}, got {top[key]!r}")

    configuration = _strict_object(
        top["configuration"],
        "configuration",
        (
            "messages",
            "warmup_messages",
            "publish_interval_us",
            "deadline_seconds",
            "clock_mode",
            "run_id",
            "runtime_dir",
            "output",
        ),
    )
    if configuration["messages"] != expected_messages:
        raise ValueError("configuration.messages mismatch")
    if configuration["warmup_messages"] != expected_warmup_messages:
        raise ValueError("configuration.warmup_messages mismatch")
    if configuration["publish_interval_us"] != expected_publish_interval_us:
        raise ValueError("configuration.publish_interval_us mismatch")
    if configuration["clock_mode"] != expected_clock_mode:
        raise ValueError("configuration.clock_mode mismatch")
    if configuration["run_id"] != expected_run_id:
        raise ValueError("configuration.run_id mismatch")
    _strict_integer(configuration["messages"], "configuration.messages", 1)
    _strict_integer(configuration["warmup_messages"], "configuration.warmup_messages")
    _strict_integer(configuration["publish_interval_us"], "configuration.publish_interval_us")
    _strict_integer(configuration["deadline_seconds"], "configuration.deadline_seconds", 1)
    if expected_deadline_seconds is not None and configuration["deadline_seconds"] != expected_deadline_seconds:
        raise ValueError("configuration.deadline_seconds mismatch")
    if not isinstance(configuration["runtime_dir"], str) or not configuration["runtime_dir"]:
        raise ValueError("configuration.runtime_dir must be a non-empty string")
    if not isinstance(configuration["output"], str) or not configuration["output"]:
        raise ValueError("configuration.output must be a non-empty string")
    if expected_runtime_directory is not None and configuration["runtime_dir"] != str(expected_runtime_directory):
        raise ValueError("configuration.runtime_dir mismatch")
    if expected_output is not None and configuration["output"] != str(expected_output):
        raise ValueError("configuration.output mismatch")

    clock = _strict_object(top["clock"], "clock", ("name", "resolution_ns", "boot_id"))
    if not isinstance(clock["name"], str) or not clock["name"]:
        raise ValueError("clock.name must be a non-empty string")
    _strict_integer(clock["resolution_ns"], "clock.resolution_ns", 1)
    if not isinstance(clock["boot_id"], str) or not clock["boot_id"]:
        raise ValueError("clock.boot_id must be a non-empty string")

    counts = _strict_object(
        top["counts"], "counts", ("offered", "received", "duplicate", "out_of_order", "corrupt", "lost")
    )
    for key in counts:
        _strict_integer(counts[key], f"counts.{key}")
    if counts["offered"] != expected_messages or counts["received"] != expected_messages:
        raise ValueError("counts.offered and counts.received must equal messages")
    for key in ("duplicate", "out_of_order", "corrupt", "lost"):
        if counts[key] != 0:
            raise ValueError(f"counts.{key} must be zero")

    latency = _strict_object(
        top["latency_ns"], "latency_ns", ("samples", "p50", "p95", "p99", "p99_9", "max")
    )
    for key in latency:
        _strict_integer(latency[key], f"latency_ns.{key}")
    elapsed = _strict_integer(top["elapsed_ns"], "elapsed_ns")
    throughput = _strict_number(
        top["throughput_messages_per_second"], "throughput_messages_per_second"
    )
    payload_bytes = _strict_integer(top["payload_bytes"], "payload_bytes", 1)
    expected_payload_bytes = {"small": 256, "medium": 65_536, "large": 1_048_576}[
        expected_profile
    ]
    if payload_bytes != expected_payload_bytes:
        raise ValueError(
            f"payload_bytes must be {expected_payload_bytes}, got {payload_bytes}"
        )
    _strict_integer(top["encoded_bytes"], "encoded_bytes", 1)
    if not isinstance(top["backend_details"], dict):
        raise ValueError("backend_details must be an object")

    if expected_role == "canbus":
        percentile_values = [
            latency["p50"],
            latency["p95"],
            latency["p99"],
            latency["p99_9"],
            latency["max"],
        ]
        if expect_sink_latency:
            if latency["samples"] != expected_messages:
                raise ValueError("sink latency_ns.samples must equal messages")
            if percentile_values != sorted(percentile_values):
                raise ValueError("sink latency percentiles must be monotonic")
        elif latency["samples"] != 0 or any(percentile_values):
            raise ValueError(
                "independent-host sink latency must be empty without clock qualification"
            )
        if expected_clock_mode == "independent-hosts" and expected_messages == 1:
            if elapsed != 0 or throughput != 0.0:
                raise ValueError(
                    "single-sample independent-host throughput must be unavailable"
                )
        else:
            if elapsed <= 0 or throughput <= 0.0:
                raise ValueError("sink elapsed_ns and throughput must be positive")
            throughput_samples = (
                expected_messages - 1
                if expected_clock_mode == "independent-hosts"
                else expected_messages
            )
            expected_throughput = throughput_samples * 1_000_000_000.0 / elapsed
            if not math.isclose(throughput, expected_throughput, rel_tol=1e-12):
                raise ValueError(
                    "sink throughput does not match its measured completion window"
                )
    else:
        if latency["samples"] != 0:
            raise ValueError("non-sink latency_ns.samples must be zero")
        if any(latency[key] != 0 for key in ("p50", "p95", "p99", "p99_9", "max")):
            raise ValueError("non-sink latency percentiles must be zero")
    return top


def _worker_command(
    binary: Path,
    spec: BackendSpec,
    role: str,
    profile: str,
    messages: int,
    warmup_messages: int,
    publish_interval_us: int,
    deadline_seconds: int,
    run_id: str,
    runtime_directory: Path,
    result_path: Path,
    domain_id: int,
    history_depth: int,
    zmq_hwm: int,
    channel_capacity: int,
) -> List[str]:
    return [str(binary)] + _common_arguments(
        profile,
        messages,
        warmup_messages,
        publish_interval_us,
        deadline_seconds,
        run_id,
        runtime_directory,
        role,
        result_path,
    ) + _backend_arguments(
        spec.name,
        domain_id,
        history_depth,
        zmq_hwm,
        channel_capacity,
        run_id,
        operation="worker",
    )


def _run_workers(
    *,
    binary: Path,
    spec: BackendSpec,
    profile: str,
    messages: int,
    warmup_messages: int,
    publish_interval_us: int,
    deadline_seconds: int,
    run_id: str,
    runtime_directory: Path,
    result_directory: Path,
    log_directory: Path,
    output_directory: Path,
    domain_id: int,
    history_depth: int,
    zmq_hwm: int,
    channel_capacity: int,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    errors: List[str] = []
    entries: List[Dict[str, Any]] = []
    processes: Dict[str, subprocess.Popen[Any]] = {}
    handles: List[Any] = []
    paths: Dict[str, Tuple[Path, Path, Path]] = {}

    for role in ROLES:
        result_path = result_directory / f"{role}.json"
        stdout_path = log_directory / f"{role}.stdout.log"
        stderr_path = log_directory / f"{role}.stderr.log"
        command = _worker_command(
            binary,
            spec,
            role,
            profile,
            messages,
            warmup_messages,
            publish_interval_us,
            deadline_seconds,
            run_id,
            runtime_directory,
            result_path,
            domain_id,
            history_depth,
            zmq_hwm,
            channel_capacity,
        )
        entries.append({"role": role, "command": command, "exit_code": None})
        paths[role] = (result_path, stdout_path, stderr_path)

    for _, stdout_path, stderr_path in paths.values():
        stdout_path.touch()
        stderr_path.touch()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        errors.append(f"binary is missing or not executable: {binary}")
    else:
        try:
            for entry in entries:
                role = entry["role"]
                _, stdout_path, stderr_path = paths[role]
                stdout = stdout_path.open("wb")
                stderr = stderr_path.open("wb")
                handles.extend((stdout, stderr))
                try:
                    processes[role] = subprocess.Popen(
                        entry["command"],
                        stdout=stdout,
                        stderr=stderr,
                        start_new_session=True,
                    )
                except OSError as exc:
                    errors.append(f"cannot launch {role}: {exc}")
                    break
            if len(processes) == len(ROLES):
                deadline = time.monotonic() + deadline_seconds
                try:
                    wait_for_readiness(
                        runtime_directory, spec.worker_backend, run_id, processes, deadline
                    )
                    _atomic_create_start(runtime_directory, run_id)
                    _wait_for_workers(processes, deadline)
                except RunFailure as exc:
                    errors.append(str(exc))
                    _terminate_process_groups(processes.values())
            else:
                _terminate_process_groups(processes.values())
        finally:
            for process in processes.values():
                if process.poll() is None:
                    _terminate_process_groups([process])
            for handle in handles:
                handle.close()

    for entry in entries:
        role = entry["role"]
        process = processes.get(role)
        entry["exit_code"] = process.poll() if process is not None else None
        result_path, stdout_path, stderr_path = paths[role]
        entry["stdout"] = _artifact(stdout_path, output_directory)
        entry["stderr"] = _artifact(stderr_path, output_directory)
        entry["result"] = _artifact(result_path, output_directory)
    return entries, errors


def _validate_run_results(
    worker_records: Sequence[Mapping[str, Any]],
    output_directory: Path,
    runtime_directory: Path,
    spec: BackendSpec,
    profile: str,
    run_id: str,
    messages: int,
    warmup_messages: int,
    publish_interval_us: int,
    deadline_seconds: int,
) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    errors: List[str] = []
    clocks: List[Tuple[str, int, str]] = []
    sink_metrics: Optional[Dict[str, Any]] = None
    seen_roles = set()
    for record in worker_records:
        role = str(record["role"])
        seen_roles.add(role)
        result_relative = record["result"]["path"]
        result_path = output_directory / result_relative
        if not result_path.is_file():
            errors.append(f"{role}: missing worker result {result_relative}")
            continue
        try:
            with result_path.open("r", encoding="utf-8") as stream:
                document = json.load(stream)
            validated = validate_worker_result(
                document,
                expected_backend=spec.worker_backend,
                expected_role=role,
                expected_profile=profile,
                expected_run_id=run_id,
                expected_messages=messages,
                expected_warmup_messages=warmup_messages,
                expected_publish_interval_us=publish_interval_us,
                expected_deadline_seconds=deadline_seconds,
                expected_runtime_directory=runtime_directory,
                expected_output=result_path,
            )
            clock = validated["clock"]
            clocks.append((clock["name"], clock["resolution_ns"], clock["boot_id"]))
            if role == "canbus":
                sink_metrics = {
                    "latency_ns": validated["latency_ns"],
                    "elapsed_ns": validated["elapsed_ns"],
                    "throughput_messages_per_second": validated[
                        "throughput_messages_per_second"
                    ],
                    "payload_bytes": validated["payload_bytes"],
                    "encoded_bytes": validated["encoded_bytes"],
                    "backend_details": validated["backend_details"],
                }
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{role}: invalid worker result: {exc}")
    missing_roles = sorted(set(ROLES) - seen_roles)
    if missing_roles:
        errors.append("worker records missing roles: " + ", ".join(missing_roles))
    if clocks and any(clock != clocks[0] for clock in clocks[1:]):
        errors.append("worker clock name/resolution/boot_id values are inconsistent")
    if sink_metrics is None:
        errors.append("validated CANBus sink metrics are missing")
    return errors, sink_metrics


def _control_command(
    binary: Path,
    operation: str,
    profile: str,
    messages: int,
    warmup_messages: int,
    publish_interval_us: int,
    deadline_seconds: int,
    run_id: str,
    runtime_directory: Path,
    result_path: Path,
    domain_id: int,
    history_depth: int,
    zmq_hwm: int,
    channel_capacity: int,
) -> List[str]:
    return [str(binary)] + _common_arguments(
        profile,
        messages,
        warmup_messages,
        publish_interval_us,
        deadline_seconds,
        run_id,
        runtime_directory,
        "perception",
        result_path,
    ) + _backend_arguments(
        "mino_shm_pipeline",
        domain_id,
        history_depth,
        zmq_hwm,
        channel_capacity,
        run_id,
        operation=operation,
    )


def execute_backend_run(
    *,
    args: argparse.Namespace,
    output_directory: Path,
    binary: Path,
    backend_name: str,
    profile: str,
    profile_index: int,
    round_index: int,
    order_index: int,
    messages: int,
    warmup_messages: int,
    publish_interval_us: int,
) -> Dict[str, Any]:
    spec = BACKEND_SPECS[backend_name]
    unique = secrets.token_hex(6)
    run_id = f"{profile}-r{round_index + 1}-{backend_name}-{unique}"
    run_directory = (
        output_directory
        / "runs"
        / profile
        / f"round-{round_index + 1:02d}"
        / backend_name
    )
    result_directory = run_directory / "results"
    log_directory = run_directory / "logs"
    result_directory.mkdir(parents=True)
    log_directory.mkdir()
    runtime_directory = _make_runtime_directory(run_id)
    domain_id = args.domain_id_base + profile_index * args.rounds + round_index
    errors: List[str] = []
    control: Dict[str, Any] = {}
    workers: List[Dict[str, Any]] = []
    sink_metrics: Optional[Dict[str, Any]] = None

    try:
        setup_succeeded = True
        if backend_name == "mino_shm_pipeline":
            setup_result = result_directory / "setup.json"
            setup_command = _control_command(
                binary,
                "setup",
                profile,
                messages,
                warmup_messages,
                publish_interval_us,
                args.deadline_seconds,
                run_id,
                runtime_directory,
                setup_result,
                domain_id,
                args.history_depth,
                args.zmq_hwm,
                args.channel_capacity,
            )
            control["setup"], setup_error = _run_control_command(
                setup_command,
                log_directory / "setup.stdout.log",
                log_directory / "setup.stderr.log",
                setup_result,
                output_directory,
                args.deadline_seconds,
            )
            if setup_error:
                setup_succeeded = False
                errors.append("Mino setup: " + setup_error)
        if setup_succeeded:
            workers, worker_errors = _run_workers(
                binary=binary,
                spec=spec,
                profile=profile,
                messages=messages,
                warmup_messages=warmup_messages,
                publish_interval_us=publish_interval_us,
                deadline_seconds=args.deadline_seconds,
                run_id=run_id,
                runtime_directory=runtime_directory,
                result_directory=result_directory,
                log_directory=log_directory,
                output_directory=output_directory,
                domain_id=domain_id,
                history_depth=args.history_depth,
                zmq_hwm=args.zmq_hwm,
                channel_capacity=args.channel_capacity,
            )
            errors.extend(worker_errors)
        else:
            workers, skipped_errors = _run_workers(
                binary=Path("/definitely/missing/mino-worker-after-setup-failure"),
                spec=spec,
                profile=profile,
                messages=messages,
                warmup_messages=warmup_messages,
                publish_interval_us=publish_interval_us,
                deadline_seconds=args.deadline_seconds,
                run_id=run_id,
                runtime_directory=runtime_directory,
                result_directory=result_directory,
                log_directory=log_directory,
                output_directory=output_directory,
                domain_id=domain_id,
                history_depth=args.history_depth,
                zmq_hwm=args.zmq_hwm,
                channel_capacity=args.channel_capacity,
            )
            errors.append("workers skipped because Mino setup failed")
            del skipped_errors
    except Exception as exc:  # Preserve the run and continue the qualification matrix.
        errors.append(f"unexpected orchestration error: {type(exc).__name__}: {exc}")
    finally:
        if backend_name == "mino_shm_pipeline" and binary.is_file() and os.access(binary, os.X_OK):
            cleanup_result = result_directory / "cleanup.json"
            cleanup_command = _control_command(
                binary,
                "cleanup",
                profile,
                messages,
                warmup_messages,
                publish_interval_us,
                args.deadline_seconds,
                run_id,
                runtime_directory,
                cleanup_result,
                domain_id,
                args.history_depth,
                args.zmq_hwm,
                args.channel_capacity,
            )
            control["cleanup"], cleanup_error = _run_control_command(
                cleanup_command,
                log_directory / "cleanup.stdout.log",
                log_directory / "cleanup.stderr.log",
                cleanup_result,
                output_directory,
                args.deadline_seconds,
            )
            if cleanup_error:
                errors.append("Mino cleanup: " + cleanup_error)

        if workers:
            validation_errors, sink_metrics = _validate_run_results(
                workers,
                output_directory,
                runtime_directory,
                spec,
                profile,
                run_id,
                messages,
                warmup_messages,
                publish_interval_us,
                args.deadline_seconds,
            )
            errors.extend(validation_errors)
        else:
            errors.append("six worker records/results are not available")
        shutil.rmtree(runtime_directory, ignore_errors=True)

    return {
        "profile": profile,
        "round": round_index + 1,
        "backend": backend_name,
        "order_index": order_index,
        "run_id": run_id,
        "outcome": "passed" if not errors else "failed",
        "errors": errors,
        "messages": messages,
        "warmup_messages": warmup_messages,
        "publish_interval_us": publish_interval_us,
        "domain_id": (
            domain_id
            if backend_name in ("fastdds_pipeline", "cyclonedds_pipeline")
            else None
        ),
        "runtime_dir": str(runtime_directory),
        "control": control,
        "workers": workers,
        "sink_metrics": sink_metrics,
    }


def _read_boot_id() -> str:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
        return value or "unavailable"
    except OSError:
        return "unavailable"


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith(("model name", "hardware")) and ":" in line:
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unavailable"


def _git_output(arguments: Sequence[str]) -> Optional[str]:
    try:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=REPOSITORY_ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _compiler_version() -> str:
    try:
        completed = subprocess.run(
            ["c++", "--version"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"
    first_line = completed.stdout.splitlines()
    return first_line[0] if completed.returncode == 0 and first_line else "unavailable"


def _collect_provenance(
    binaries: Mapping[str, Path], requested_commit: Optional[str]
) -> Tuple[Dict[str, Any], List[str]]:
    errors: List[str] = []
    detected_commit = _git_output(("rev-parse", "HEAD"))
    status = _git_output(("status", "--porcelain", "--untracked-files=normal"))
    if (
        requested_commit is not None
        and detected_commit is not None
        and requested_commit != detected_commit
    ):
        errors.append(
            f"--commit {requested_commit} does not match HEAD {detected_commit}"
        )
    binary_hashes: Dict[str, Any] = {}
    for name, path in binaries.items():
        if path.is_file():
            binary_hashes[name] = {"path": str(path), "sha256": _sha256(path)}
        else:
            binary_hashes[name] = {"path": str(path), "sha256": None}
            errors.append(f"binary does not exist: {path}")
    schema_hashes: Dict[str, Any] = {}
    for path in SCHEMA_PATHS:
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        if path.is_file():
            schema_hashes[relative] = _sha256(path)
        else:
            schema_hashes[relative] = None
            errors.append(f"schema does not exist: {relative}")
    build_input_hashes: Dict[str, Any] = {}
    for path in BUILD_INPUT_PATHS:
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        if path.is_file():
            build_input_hashes[relative] = _sha256(path)
        else:
            build_input_hashes[relative] = None
            errors.append(f"build input does not exist: {relative}")
    return (
        {
            "utc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "hostname": socket.gethostname(),
            "boot_id": _read_boot_id(),
            "platform": platform.platform(),
            "kernel": platform.release(),
            "cpu_model": _cpu_model(),
            "cpu_count": os.cpu_count(),
            "compiler": _compiler_version(),
            "git": {
                "commit": requested_commit or detected_commit or "unavailable",
                "commit_source": "--commit" if requested_commit else "git",
                "detected_commit": detected_commit or "unavailable",
                "dirty": None if status is None else bool(status),
            },
            "binary_hashes": binary_hashes,
            "schema_hashes": schema_hashes,
            "build_input_hashes": build_input_hashes,
        },
        errors,
    )


def _resolved_binaries(args: argparse.Namespace) -> Dict[str, Path]:
    all_binaries = {
        "fastdds_pipeline": args.fastdds_binary.expanduser().resolve(),
        "cyclonedds_pipeline": args.cyclonedds_binary.expanduser().resolve(),
        "protobuf_zmq_pipeline": args.protobuf_zmq_binary.expanduser().resolve(),
        "mino_shm_pipeline": args.mino_shm_binary.expanduser().resolve(),
    }
    return {name: all_binaries[name] for name in args.backends}


def _messages_for_profile(args: argparse.Namespace, profile: str) -> int:
    return int(getattr(args, f"{profile}_messages"))


def _publish_interval_for_profile(args: argparse.Namespace, profile: str) -> int:
    return int(getattr(args, f"{profile}_publish_interval_us"))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if args.channel_capacity & (args.channel_capacity - 1):
        parser.error("--channel-capacity must be a power of two")
    maximum_domain_id = args.domain_id_base + len(args.profiles) * args.rounds - 1
    if maximum_domain_id > 232:
        parser.error(
            "--domain-id-base plus the selected profile/round matrix must not exceed 232"
        )
    try:
        output_directory = prepare_output_directory(args.output_dir)
    except (OSError, RunnerConfigurationError) as exc:
        print(f"pipeline comparison configuration error: {exc}", file=sys.stderr)
        return 2

    binaries = _resolved_binaries(args)
    provenance, provenance_errors = _collect_provenance(binaries, args.commit)
    manifest: Dict[str, Any] = {
        "schema": MANIFEST_SCHEMA,
        "outcome": "failed",
        "errors": list(provenance_errors),
        "methodology": METHODOLOGY,
        "config": {
            "rounds": args.rounds,
            "backends": list(args.backends),
            "profiles": list(args.profiles),
            "messages": {
                profile: _messages_for_profile(args, profile) for profile in args.profiles
            },
            "warmup_ratio": args.warmup_ratio,
            "publish_interval_us": {
                profile: _publish_interval_for_profile(args, profile)
                for profile in args.profiles
            },
            "load_mode": {
                profile: (
                    "saturation"
                    if _publish_interval_for_profile(args, profile) == 0
                    else "paced_latency"
                )
                for profile in args.profiles
            },
            "warmup_messages": {
                profile: int(_messages_for_profile(args, profile) * args.warmup_ratio + 0.5)
                for profile in args.profiles
            },
            "deadline_seconds": args.deadline_seconds,
            "seed": args.seed,
            "domain_id_base": args.domain_id_base,
            "history_depth": args.history_depth,
            "zmq_hwm": args.zmq_hwm,
            "channel_capacity": args.channel_capacity,
            "fail_fast": args.fail_fast,
            "binaries": {name: str(path) for name, path in binaries.items()},
            "commit_override": args.commit,
        },
        "backend_order": [],
        "dependency_labels": {
            name: list(BACKEND_SPECS[name].dependency_labels)
            for name in args.backends
        },
        "provenance": provenance,
        "runs": [],
    }

    stop = False
    try:
        for profile_index, profile in enumerate(args.profiles):
            messages = _messages_for_profile(args, profile)
            warmup_messages = int(messages * args.warmup_ratio + 0.5)
            publish_interval_us = _publish_interval_for_profile(args, profile)
            for round_index in range(args.rounds):
                order = backend_order(
                    args.seed, profile, round_index, args.backends
                )
                manifest["backend_order"].append(
                    {"profile": profile, "round": round_index + 1, "order": list(order)}
                )
                for order_index, backend_name in enumerate(order):
                    run = execute_backend_run(
                        args=args,
                        output_directory=output_directory,
                        binary=binaries[backend_name],
                        backend_name=backend_name,
                        profile=profile,
                        profile_index=profile_index,
                        round_index=round_index,
                        order_index=order_index,
                        messages=messages,
                        warmup_messages=warmup_messages,
                        publish_interval_us=publish_interval_us,
                    )
                    manifest["runs"].append(run)
                    if run["errors"]:
                        manifest["errors"].extend(
                            f"{profile}/round-{round_index + 1}/{backend_name}: {error}"
                            for error in run["errors"]
                        )
                        if args.fail_fast:
                            stop = True
                            break
                if stop:
                    break
            if stop:
                break
    except Exception as exc:  # The manifest remains the authoritative failure artifact.
        manifest["errors"].append(
            f"runner failed unexpectedly: {type(exc).__name__}: {exc}"
        )

    manifest["outcome"] = "passed" if not manifest["errors"] else "failed"
    manifest["provenance"]["completed_utc"] = (
        dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    )
    try:
        _atomic_write_json(output_directory / "manifest.json", manifest)
    except OSError as exc:
        print(f"failed to write manifest: {exc}", file=sys.stderr)
        return 1
    return 0 if manifest["outcome"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
