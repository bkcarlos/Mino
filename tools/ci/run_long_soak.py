#!/usr/bin/env python3
"""Long-soak steady-state qualification runner with fail-closed evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA = "mino.long_soak.steady_state_qualification.v1"
MARKER = ".mino-long-soak-steady-state-qualification-evidence"
DEFAULT_DURATION_SECONDS = 72 * 60 * 60
DAY_SECONDS = 24 * 60 * 60
GROWTH_LIMIT_PERCENT = 5.0
REQUIRED_WORKLOADS = ("allocator", "channel", "bridge", "storage", "observability")
SAMPLE_KEYS = (
    "allocator_occupied_slots",
    "allocator_total_slots",
    "slab_bytes_in_use",
    "channel_queue_depth",
    "storage_queue_depth",
    "queue_depth",
    "storage_bytes_in_use",
    "storage_allocated_bytes",
    "telemetry_accepted",
    "telemetry_dropped",
    "operations",
)

_received_signal = 0


def _signal_handler(signum: int, _frame: Any) -> None:
    global _received_signal
    _received_signal = signum


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json_atomic(path: Path, value: Any) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump(value, destination, indent=2, sort_keys=True)
            destination.write("\n")
            destination.flush()
            os.fsync(destination.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _git_output(command: Sequence[str], workspace: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", *command],
            cwd=workspace,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError(f"git provenance command failed: {error}") from error
    if result.returncode != 0:
        raise ValueError(
            f"git provenance command failed ({shlex.join(command)}): "
            f"{result.stdout.strip()}"
        )
    return result.stdout.strip()


def _source_provenance(workspace: Path) -> tuple[str, str, list[str]]:
    commit = _git_output(["rev-parse", "HEAD"], workspace)
    if len(commit) != 40:
        raise ValueError(f"git returned an invalid commit: {commit!r}")
    status = _git_output(
        ["status", "--porcelain=v1", "--untracked-files=all"], workspace
    )
    status_lines = status.splitlines() if status else []
    return commit, ("clean" if not status_lines else "dirty"), status_lines


def _environment(environment: Mapping[str, str]) -> dict[str, Any]:
    github_names = (
        "GITHUB_ACTIONS",
        "GITHUB_EVENT_NAME",
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_REPOSITORY",
        "GITHUB_RUN_ATTEMPT",
        "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER",
        "GITHUB_SHA",
        "GITHUB_WORKFLOW",
        "RUNNER_ARCH",
        "RUNNER_NAME",
        "RUNNER_OS",
    )
    return {
        "os": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "hostname": platform.node(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
        "cc": environment.get("CC"),
        "cxx": environment.get("CXX"),
        "github": {name.lower(): environment.get(name) for name in github_names},
    }


def _prepare_output(output: Path, clean: bool) -> None:
    if output.exists():
        if output.is_symlink() or not output.is_dir():
            raise ValueError(f"output is not a real directory: {output}")
        if any(output.iterdir()):
            if not clean:
                raise ValueError(f"output is not empty: {output}")
            if not (output / MARKER).is_file():
                raise ValueError(f"refusing to clean unmarked output: {output}")
            shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / MARKER).write_text(
        "Mino long-soak steady-state qualification evidence\n", encoding="ascii"
    )


class ProcessSampler:
    def __init__(self, pid: int):
        self.pid = pid
        self.system = platform.system()
        self.lsof = shutil.which("lsof")

    def sample(self) -> tuple[int, int]:
        if self.system == "Linux":
            return self._linux()
        if self.system == "Darwin":
            return self._darwin()
        raise RuntimeError(f"process sampling is unsupported on {self.system}")

    def _linux(self) -> tuple[int, int]:
        status_path = Path(f"/proc/{self.pid}/status")
        rss_bytes: int | None = None
        for line in status_path.read_text(encoding="ascii").splitlines():
            if line.startswith("VmRSS:"):
                fields = line.split()
                if len(fields) != 3 or fields[2] != "kB":
                    raise RuntimeError("unexpected /proc VmRSS format")
                rss_bytes = int(fields[1]) * 1024
                break
        if rss_bytes is None:
            raise RuntimeError("VmRSS is absent from /proc status")
        fd_count = sum(1 for _ in Path(f"/proc/{self.pid}/fd").iterdir())
        return rss_bytes, fd_count

    def _darwin(self) -> tuple[int, int]:
        rss = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(self.pid)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        if rss.returncode != 0 or not rss.stdout.strip():
            raise RuntimeError(f"ps RSS sampling failed: {rss.stderr.strip()}")
        if self.lsof is None:
            raise RuntimeError("lsof is required for FD sampling on macOS")
        fds = subprocess.run(
            [self.lsof, "-a", "-p", str(self.pid), "-Ff"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        if fds.returncode != 0:
            raise RuntimeError(f"lsof FD sampling failed: {fds.stderr.strip()}")
        fd_count = sum(
            1
            for line in fds.stdout.splitlines()
            if line.startswith("f") and line[1:].isdigit()
        )
        return int(rss.stdout.strip()) * 1024, fd_count


def _growth(values: Sequence[tuple[float, float]]) -> dict[str, Any]:
    if len(values) < 3:
        return {
            "sample_count": len(values),
            "status": "insufficient_samples",
            "linear_percent_per_24h": None,
            "endpoint_percent_per_24h": None,
        }
    first_time, first_value = values[0]
    last_time, last_value = values[-1]
    elapsed = last_time - first_time
    if elapsed <= 0:
        return {
            "sample_count": len(values),
            "status": "invalid_time_range",
            "linear_percent_per_24h": None,
            "endpoint_percent_per_24h": None,
        }
    times = [point[0] - first_time for point in values]
    observations = [point[1] for point in values]
    mean_time = statistics.fmean(times)
    mean_value = statistics.fmean(observations)
    denominator = sum((value - mean_time) ** 2 for value in times)
    slope = (
        sum(
            (time_value - mean_time) * (observation - mean_value)
            for time_value, observation in zip(times, observations)
        )
        / denominator
        if denominator > 0
        else 0.0
    )
    baseline = max(abs(first_value), 1.0)
    linear = slope * DAY_SECONDS / baseline * 100.0
    endpoint = (last_value - first_value) / baseline * DAY_SECONDS / elapsed * 100.0
    return {
        "sample_count": len(values),
        "status": "computed",
        "first_value": first_value,
        "last_value": last_value,
        "elapsed_seconds": elapsed,
        "linear_slope_units_per_second": slope,
        "linear_percent_per_24h": linear,
        "endpoint_percent_per_24h": endpoint,
    }


def _analyse(samples: Sequence[dict[str, Any]], warmup_seconds: float,
             requested_duration: float) -> dict[str, Any]:
    eligible = [sample for sample in samples if sample["elapsed_seconds"] >= warmup_seconds]
    rss = _growth([(sample["elapsed_seconds"], sample["rss_bytes"]) for sample in eligible])
    slab = _growth(
        [(sample["elapsed_seconds"], sample["slab_bytes_in_use"]) for sample in eligible]
    )
    applicable = requested_duration >= DAY_SECONDS
    checks: list[dict[str, Any]] = []
    for metric, trend in (("rss", rss), ("slab", slab)):
        for method in ("linear", "endpoint"):
            value = trend[f"{method}_percent_per_24h"]
            passed = (
                value is not None
                and math.isfinite(value)
                and value < GROWTH_LIMIT_PERCENT
            )
            checks.append(
                {
                    "metric": metric,
                    "method": method,
                    "observed_percent_per_24h": value,
                    "limit_percent_per_24h": GROWTH_LIMIT_PERCENT,
                    "comparison": "<",
                    "passed": passed if applicable else None,
                }
            )
    return {
        "warmup_seconds": warmup_seconds,
        "eligible_sample_count": len(eligible),
        "gate_applicable": applicable,
        "gate_status": (
            "passed"
            if applicable and all(check["passed"] for check in checks)
            else "failed"
            if applicable
            else "not_evaluated_short_run"
        ),
        "rss": rss,
        "slab": slab,
        "checks": checks,
    }


class ProbeReader(threading.Thread):
    def __init__(self, stream: Any, log_path: Path):
        super().__init__(name="soak-probe-reader", daemon=True)
        self.stream = stream
        self.log_path = log_path
        self.lock = threading.Lock()
        self.latest_sample: dict[str, Any] | None = None
        self.last_heartbeat = time.monotonic()
        self.ready_workloads: tuple[str, ...] | None = None
        self.malformed_samples: list[str] = []

    def run(self) -> None:
        with self.log_path.open("w", encoding="utf-8", newline="\n") as log:
            for line in self.stream:
                log.write(line)
                log.flush()
                stripped = line.strip()
                if not stripped.startswith("{"):
                    continue
                try:
                    value = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                with self.lock:
                    if value.get("type") == "ready":
                        workloads = value.get("workloads")
                        if isinstance(workloads, list) and all(
                            isinstance(item, str) for item in workloads
                        ):
                            self.ready_workloads = tuple(workloads)
                            self.last_heartbeat = time.monotonic()
                    elif value.get("type") == "sample":
                        if all(
                            isinstance(value.get(key), int) and value[key] >= 0
                            for key in SAMPLE_KEYS
                        ):
                            self.latest_sample = value
                            self.last_heartbeat = time.monotonic()
                        else:
                            self.malformed_samples.append(stripped)
            log.flush()
            os.fsync(log.fileno())

    def snapshot(self) -> tuple[dict[str, Any] | None, float, tuple[str, ...] | None, int]:
        with self.lock:
            sample = None if self.latest_sample is None else dict(self.latest_sample)
            return sample, self.last_heartbeat, self.ready_workloads, len(self.malformed_samples)


def _terminate_process_group(process: subprocess.Popen[str], grace_seconds: float) -> int:
    if process.poll() is not None:
        return int(process.returncode)
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        pass
    try:
        return process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            pass
        return process.wait(timeout=10)


def _append_sample(path: Path, sample: Mapping[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as destination:
        json.dump(sample, destination, sort_keys=True, separators=(",", ":"))
        destination.write("\n")
        destination.flush()
        os.fsync(destination.fileno())


def _base_manifest(
    *,
    commit: str | None,
    expected_commit: str | None,
    source_state: str,
    source_status: Sequence[str],
    seed: int,
    command: Sequence[str],
    duration_seconds: float,
    sample_interval_seconds: float,
    warmup_seconds: float,
    watchdog_seconds: float,
    environment: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "commit": commit,
        "expected_commit": expected_commit,
        "source_state": source_state,
        "source_status": list(source_status),
        "qualification_eligible": False,
        "seed": seed,
        "seed_consumed": True,
        "command": list(command),
        "command_shell": shlex.join(command),
        "environment": _environment(environment),
        "workloads": list(REQUIRED_WORKLOADS),
        "configuration": {
            "requested_duration_seconds": duration_seconds,
            "sample_interval_seconds": sample_interval_seconds,
            "warmup_seconds": warmup_seconds,
            "watchdog_seconds": watchdog_seconds,
            "growth_limit_percent_per_24h": GROWTH_LIMIT_PERCENT,
            "growth_limit_comparison": "<",
        },
        "started_at": _utc_now(),
        "finished_at": None,
        "elapsed_seconds": 0.0,
        "samples": [],
        "analysis": None,
        "logs": [],
        "termination_reason": None,
        "termination_signal": None,
        "probe_exit_code": None,
        "exit_reason": "runner_active",
        "outcome": "running",
        "artifact_complete": False,
        "evidence_errors": [],
    }


def run_campaign(args: argparse.Namespace, workspace: Path) -> tuple[int, Path]:
    output = Path(args.out).expanduser().resolve()
    commit, source_state, source_status = _source_provenance(workspace)
    expected_commit = args.expected_commit.lower() if args.expected_commit else None
    if commit is not None:
        commit = commit.lower()
    if expected_commit is not None and commit != expected_commit:
        raise ValueError(f"commit mismatch: HEAD={commit}, expected={expected_commit}")
    if source_state != "clean" and not args.allow_dirty:
        raise ValueError("source tree is dirty; use --allow-dirty only for local smoke")
    _prepare_output(output, args.clean)

    probe = Path(args.probe).expanduser()
    if not probe.is_absolute():
        probe = (workspace / probe).resolve()
    if not probe.is_file() or not os.access(probe, os.X_OK):
        raise ValueError(f"probe is not an executable file: {probe}")
    report_interval_ms = max(1, int(args.sample_interval_seconds * 500))
    command = [
        str(probe),
        f"--seed={args.seed}",
        f"--report-interval-ms={report_interval_ms}",
    ]
    watchdog_seconds = (
        args.watchdog_seconds
        if args.watchdog_seconds is not None
        else max(30.0, args.sample_interval_seconds * 3.0)
    )
    manifest = _base_manifest(
        commit=commit,
        expected_commit=expected_commit,
        source_state=source_state,
        source_status=source_status,
        seed=args.seed,
        command=command,
        duration_seconds=args.duration_seconds,
        sample_interval_seconds=args.sample_interval_seconds,
        warmup_seconds=args.warmup_seconds,
        watchdog_seconds=watchdog_seconds,
        environment=os.environ,
    )
    manifest_path = output / "manifest.json"
    samples_path = output / "samples.jsonl"
    log_path = output / "probe.log"
    samples_path.touch()
    _write_json_atomic(manifest_path, manifest)

    process: subprocess.Popen[str] | None = None
    reader: ProbeReader | None = None
    termination_reason = "runner_error"
    evidence_errors: list[str] = []
    start = time.monotonic()
    probe_exit_code: int | None = None
    try:
        process = subprocess.Popen(
            command,
            cwd=workspace,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=(os.name == "posix"),
        )
        assert process.stdout is not None
        reader = ProbeReader(process.stdout, log_path)
        reader.start()
        sampler = ProcessSampler(process.pid)
        next_sample = start
        while True:
            now = time.monotonic()
            elapsed = now - start
            latest, last_heartbeat, ready, malformed = reader.snapshot()
            if _received_signal:
                termination_reason = "signal"
                break
            return_code = process.poll()
            if return_code is not None:
                probe_exit_code = int(return_code)
                termination_reason = "probe_exit"
                break
            if now - last_heartbeat > watchdog_seconds:
                evidence_errors.append(
                    f"probe heartbeat exceeded watchdog ({watchdog_seconds}s)"
                )
                termination_reason = "watchdog_timeout"
                break
            if elapsed >= args.duration_seconds:
                termination_reason = "duration_completed"
                break
            if now >= next_sample:
                if latest is None:
                    if elapsed > min(watchdog_seconds, 10.0):
                        evidence_errors.append("probe emitted no valid metric sample")
                        termination_reason = "invalid_probe_metrics"
                        break
                else:
                    try:
                        rss_bytes, fd_count = sampler.sample()
                    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                        evidence_errors.append(f"process sampling failed: {error}")
                        termination_reason = "sampling_failed"
                        break
                    sample = {
                        "index": len(manifest["samples"]),
                        "timestamp": _utc_now(),
                        "elapsed_seconds": elapsed,
                        "rss_bytes": rss_bytes,
                        "fd_count": fd_count,
                        "probe_elapsed_ms": latest["elapsed_ms"],
                        **{key: latest[key] for key in SAMPLE_KEYS},
                    }
                    manifest["samples"].append(sample)
                    _append_sample(samples_path, sample)
                    manifest["elapsed_seconds"] = elapsed
                    _write_json_atomic(manifest_path, manifest)
                next_sample += args.sample_interval_seconds
                if next_sample <= now:
                    next_sample = now + args.sample_interval_seconds
            if ready is not None and set(ready) != set(REQUIRED_WORKLOADS):
                evidence_errors.append(f"probe workload set mismatch: {ready}")
                termination_reason = "workload_mismatch"
                break
            if malformed:
                evidence_errors.append(f"probe emitted {malformed} malformed metric samples")
                termination_reason = "invalid_probe_metrics"
                break
            time.sleep(min(0.05, max(0.005, args.sample_interval_seconds / 10.0)))
    except (OSError, RuntimeError) as error:
        evidence_errors.append(f"runner exception: {error}")
        termination_reason = "runner_error"
    finally:
        if process is not None:
            probe_exit_code = _terminate_process_group(process, args.terminate_grace_seconds)
        if reader is not None:
            reader.join(timeout=10)
            if reader.is_alive():
                evidence_errors.append("probe log reader did not terminate")
        if not log_path.exists():
            log_path.write_text("", encoding="utf-8")

    elapsed = time.monotonic() - start
    manifest["elapsed_seconds"] = elapsed
    manifest["finished_at"] = _utc_now()
    manifest["termination_reason"] = termination_reason
    manifest["termination_signal"] = (
        signal.Signals(_received_signal).name if _received_signal else None
    )
    manifest["probe_exit_code"] = probe_exit_code
    manifest["analysis"] = _analyse(
        manifest["samples"], args.warmup_seconds, args.duration_seconds
    )
    if len(manifest["samples"]) < 3:
        evidence_errors.append("fewer than three complete resource samples")
    if probe_exit_code != 0:
        evidence_errors.append(f"probe exit code was {probe_exit_code}, expected 0")
    if reader is not None:
        _, _, ready, malformed = reader.snapshot()
        if ready is None or set(ready) != set(REQUIRED_WORKLOADS):
            evidence_errors.append("probe did not attest the required workload set")
        if malformed:
            evidence_errors.append(f"probe emitted {malformed} malformed metric samples")
    if termination_reason != "duration_completed":
        evidence_errors.append(f"run ended because {termination_reason}")
    if manifest["analysis"]["gate_status"] == "failed":
        evidence_errors.append("RSS/Slab growth gate failed")

    manifest["qualification_eligible"] = bool(
        source_state == "clean"
        and expected_commit is not None
        and commit == expected_commit
        and args.duration_seconds >= DEFAULT_DURATION_SECONDS
        and elapsed >= args.duration_seconds
    )
    manifest["logs"] = [
        {
            "path": log_path.name,
            "bytes": log_path.stat().st_size,
            "sha256": _sha256(log_path),
        },
        {
            "path": samples_path.name,
            "bytes": samples_path.stat().st_size,
            "sha256": _sha256(samples_path),
        },
    ]
    manifest["evidence_errors"] = sorted(set(evidence_errors))
    manifest["outcome"] = "passed" if not evidence_errors else "failed"
    if manifest["outcome"] == "passed":
        manifest["exit_reason"] = "completed"
    elif manifest["analysis"]["gate_status"] == "failed":
        manifest["exit_reason"] = "growth_gate_failed"
    else:
        manifest["exit_reason"] = termination_reason
    manifest["artifact_complete"] = True
    _write_json_atomic(manifest_path, manifest)

    integrity_errors = verify_manifest(
        manifest_path,
        expected_commit=expected_commit,
        require_passed=False,
        require_qualified=False,
    )
    if integrity_errors:
        manifest["artifact_complete"] = False
        manifest["outcome"] = "failed"
        manifest["exit_reason"] = "artifact_verification_failed"
        manifest["evidence_errors"] = sorted(
            set(manifest["evidence_errors"] + integrity_errors)
        )
        _write_json_atomic(manifest_path, manifest)
        return 1, manifest_path
    return (0 if manifest["outcome"] == "passed" else 1), manifest_path


def verify_manifest(
    manifest_path: Path,
    *,
    expected_commit: str | None,
    require_passed: bool,
    require_qualified: bool,
) -> list[str]:
    errors: list[str] = []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return [f"manifest unreadable: {error}"]
    root = manifest_path.parent
    if manifest.get("schema") != SCHEMA:
        errors.append("manifest schema mismatch")
    if manifest.get("artifact_complete") is not True:
        errors.append("artifact_complete is not true")
    if expected_commit is not None and manifest.get("commit") != expected_commit.lower():
        errors.append("manifest commit does not match expected commit")
    if require_passed and manifest.get("outcome") != "passed":
        errors.append("manifest outcome is not passed")
    if require_qualified and manifest.get("qualification_eligible") is not True:
        errors.append("manifest is not qualification eligible")
    if set(manifest.get("workloads", [])) != set(REQUIRED_WORKLOADS):
        errors.append("manifest workload set is incomplete")
    samples = manifest.get("samples")
    if not isinstance(samples, list):
        errors.append("manifest samples are absent")
        samples = []
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict) or sample.get("index") != index:
            errors.append(f"sample {index} is malformed or out of order")
            continue
        for key in ("rss_bytes", "fd_count", *SAMPLE_KEYS):
            if not isinstance(sample.get(key), (int, float)) or sample[key] < 0:
                errors.append(f"sample {index} has invalid {key}")
    logs = manifest.get("logs")
    if not isinstance(logs, list) or {item.get("path") for item in logs if isinstance(item, dict)} != {
        "probe.log",
        "samples.jsonl",
    }:
        errors.append("evidence file set is incomplete")
        logs = []
    for item in logs:
        path = root / item["path"]
        if not path.is_file():
            errors.append(f"evidence file is absent: {item['path']}")
            continue
        if path.stat().st_size != item.get("bytes"):
            errors.append(f"evidence size mismatch: {item['path']}")
        if _sha256(path) != item.get("sha256"):
            errors.append(f"evidence hash mismatch: {item['path']}")
    samples_path = root / "samples.jsonl"
    if samples_path.is_file():
        try:
            file_samples = [
                json.loads(line)
                for line in samples_path.read_text(encoding="utf-8").splitlines()
                if line
            ]
            if file_samples != samples:
                errors.append("samples.jsonl does not exactly match manifest samples")
        except json.JSONDecodeError as error:
            errors.append(f"samples.jsonl is malformed: {error}")
    return errors


def _fake_probe_main(mode: str) -> int:
    stopped = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(json.dumps({"type": "ready", "workloads": list(REQUIRED_WORKLOADS)}), flush=True)
    if mode == "hang":
        while not stopped:
            time.sleep(0.05)
        return 0
    start = time.monotonic()
    operations = 0
    while not stopped:
        operations += 100
        elapsed_ms = int((time.monotonic() - start) * 1000)
        print(
            json.dumps(
                {
                    "type": "sample",
                    "elapsed_ms": elapsed_ms,
                    "operations": operations,
                    "allocator_occupied_slots": 64,
                    "allocator_total_slots": 704,
                    "slab_bytes_in_use": 65536,
                    "channel_queue_depth": 16,
                    "storage_queue_depth": 16,
                    "queue_depth": 32,
                    "storage_bytes_in_use": 65536,
                    "storage_allocated_bytes": 65536,
                    "telemetry_accepted": operations,
                    "telemetry_dropped": 0,
                    "metric_counters": 1,
                },
                separators=(",", ":"),
            ),
            flush=True,
        )
        time.sleep(0.03)
    return 0


def _self_test(workspace: Path) -> int:
    low = _growth([(0.0, 100.0), (43200.0, 102.0), (86400.0, 104.0)])
    high = _growth([(0.0, 100.0), (43200.0, 103.0), (86400.0, 106.0)])
    assert low["linear_percent_per_24h"] < GROWTH_LIMIT_PERCENT
    assert low["endpoint_percent_per_24h"] < GROWTH_LIMIT_PERCENT
    assert high["linear_percent_per_24h"] >= GROWTH_LIMIT_PERCENT
    assert high["endpoint_percent_per_24h"] >= GROWTH_LIMIT_PERCENT

    with tempfile.TemporaryDirectory(prefix="mino-soak-self-test-") as temporary:
        root = Path(temporary)
        fake = root / "fake-probe"
        fake.write_text(
            "#!/bin/sh\nexec "
            + shlex.quote(sys.executable)
            + " "
            + shlex.quote(str(Path(__file__).resolve()))
            + " --internal-fake-probe steady\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        args = argparse.Namespace(
            out=str(root / "steady"),
            expected_commit=None,
            allow_dirty=True,
            clean=False,
            probe=str(fake),
            seed=17,
            duration_seconds=0.8,
            sample_interval_seconds=0.1,
            warmup_seconds=0.0,
            watchdog_seconds=0.4,
            terminate_grace_seconds=1.0,
        )
        code, manifest_path = run_campaign(args, workspace)
        assert code == 0, manifest_path.read_text(encoding="utf-8")
        assert not verify_manifest(
            manifest_path,
            expected_commit=None,
            require_passed=True,
            require_qualified=False,
        )
        samples_path = manifest_path.parent / "samples.jsonl"
        original = samples_path.read_bytes()
        samples_path.write_bytes(original + b"{}\n")
        assert verify_manifest(
            manifest_path,
            expected_commit=None,
            require_passed=False,
            require_qualified=False,
        )
        samples_path.write_bytes(original)

        hanging = root / "hanging-probe"
        hanging.write_text(
            "#!/bin/sh\nexec "
            + shlex.quote(sys.executable)
            + " "
            + shlex.quote(str(Path(__file__).resolve()))
            + " --internal-fake-probe hang\n",
            encoding="utf-8",
        )
        hanging.chmod(0o755)
        args.out = str(root / "hang")
        args.probe = str(hanging)
        args.duration_seconds = 2.0
        args.watchdog_seconds = 0.25
        code, manifest_path = run_campaign(args, workspace)
        assert code == 1
        failed = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert failed["termination_reason"] == "watchdog_timeout", failed
        assert failed["artifact_complete"] is True
        assert not verify_manifest(
            manifest_path,
            expected_commit=None,
            require_passed=False,
            require_qualified=False,
        )
    print("run_long_soak.py self-test: PASS")
    return 0


def _parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--internal-fake-probe", choices=("steady", "hang"))
    parser.add_argument("--verify-manifest", type=Path)
    parser.add_argument("--require-passed", action="store_true")
    parser.add_argument("--require-qualified", action="store_true")
    parser.add_argument("--expected-commit")
    parser.add_argument("--duration-seconds", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--sample-interval-seconds", type=float, default=60.0)
    parser.add_argument("--warmup-seconds", type=float, default=300.0)
    parser.add_argument("--watchdog-seconds", type=float)
    parser.add_argument("--terminate-grace-seconds", type=float, default=10.0)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--probe", default="bazel-bin/benchmarks/soak_probe/soak_probe"
    )
    parser.add_argument("--out", default="long-soak-evidence")
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args(argv)
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be positive")
    if args.sample_interval_seconds < 0.05:
        parser.error("--sample-interval-seconds must be at least 0.05")
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be non-negative")
    if args.watchdog_seconds is not None and args.watchdog_seconds < 0.1:
        parser.error("--watchdog-seconds must be at least 0.1")
    if args.terminate_grace_seconds <= 0:
        parser.error("--terminate-grace-seconds must be positive")
    if args.seed < 0 or args.seed > (1 << 64) - 1:
        parser.error("--seed must be a uint64")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_arguments(sys.argv[1:] if argv is None else argv)
    if args.internal_fake_probe:
        return _fake_probe_main(args.internal_fake_probe)
    workspace = Path(__file__).resolve().parents[2]
    if args.self_test:
        return _self_test(workspace)
    if args.verify_manifest:
        errors = verify_manifest(
            args.verify_manifest.resolve(),
            expected_commit=args.expected_commit,
            require_passed=args.require_passed,
            require_qualified=args.require_qualified,
        )
        if errors:
            for error in errors:
                print(f"verification error: {error}", file=sys.stderr)
            return 1
        print(f"verified complete soak evidence: {args.verify_manifest}")
        return 0

    previous_handlers: dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, _signal_handler)
    try:
        code, manifest_path = run_campaign(args, workspace)
        print(f"soak manifest: {manifest_path}")
        return code
    except (OSError, ValueError) as error:
        print(f"run_long_soak.py: {error}", file=sys.stderr)
        return 2
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
