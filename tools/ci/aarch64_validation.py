#!/usr/bin/env python3
"""Fail-closed native Linux AArch64 qualification runner for V-13."""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from typing import Any


SCHEMA = "mino.aarch64.qualification_manifest.v1"
TEST_GROUPS = {
    "atomic_litmus": "//tests/aarch64:atomic_litmus_tests",
    "abi": "//tests/aarch64:abi_tests",
    "region": "//tests/aarch64:region_tests",
    "mpmc": "//tests/aarch64:mpmc_tests",
    "channel": "//tests/aarch64:channel_tests",
    "bridge": "//tests/aarch64:bridge_tests",
    "storage": "//tests/aarch64:storage_tests",
}
BENCHMARK_TARGETS = [
    "//benchmarks/allocator:allocator_benchmark",
    "//benchmarks:validation_benchmark",
    "//benchmarks:storage_benchmark",
    "//benchmarks/transport:layer_comparison_benchmark",
    "//mino/observability:v23_telemetry_benchmark",
]


class QualificationError(RuntimeError):
    pass


def run_text(command: list[str], cwd: Path, timeout: int = 30) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise QualificationError(
            f"command failed ({completed.returncode}): {shlex.join(command)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact(path: Path, output_dir: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size == 0:
        raise QualificationError(f"required artifact is missing or empty: {path}")
    return {
        "path": path.relative_to(output_dir).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def recursive_numeric_values(value: Any, key: str) -> Iterable[float]:
    if isinstance(value, dict):
        for child_key, child in value.items():
            if child_key == key and isinstance(child, (int, float)):
                yield float(child)
            yield from recursive_numeric_values(child, key)
    elif isinstance(value, list):
        for child in value:
            yield from recursive_numeric_values(child, key)


def qemu_indicators(environment: Mapping[str, str] | None = None) -> list[str]:
    environment = os.environ if environment is None else environment
    evidence = {
        "/proc/cpuinfo": read_text(Path("/proc/cpuinfo")),
        "/sys/firmware/devicetree/base/compatible": read_text(
            Path("/sys/firmware/devicetree/base/compatible")
        ),
        "/sys/devices/virtual/dmi/id/product_name": read_text(
            Path("/sys/devices/virtual/dmi/id/product_name")
        ),
        "/sys/devices/virtual/dmi/id/sys_vendor": read_text(
            Path("/sys/devices/virtual/dmi/id/sys_vendor")
        ),
    }
    indicators = []
    for source, value in evidence.items():
        lowered = value.lower()
        if "qemu" in lowered or "tcg" in lowered:
            indicators.append(f"{source} identifies QEMU/TCG")
    for name in ("QEMU_CPU", "QEMU_LD_PREFIX"):
        if environment.get(name):
            indicators.append(f"{name} is set")
    detector = shutil.which("systemd-detect-virt")
    if detector:
        detected = subprocess.run(
            [detector, "--vm"], check=False, capture_output=True, text=True
        ).stdout.strip()
        if detected.lower() == "qemu":
            indicators.append("systemd-detect-virt reports qemu")
    return indicators


def collect_governors(root: Path = Path("/sys/devices/system/cpu")) -> dict[str, str]:
    governors: dict[str, str] = {}
    for path in sorted(root.glob("cpu[0-9]*/cpufreq/scaling_governor")):
        value = read_text(path)
        if value:
            governors[path.parent.parent.name] = value
    return governors


def evaluate_preflight(
    *,
    system: str,
    machine: str,
    commit: str,
    expected_commit: str,
    status: str,
    governors: dict[str, str],
    expected_governor: str,
    qemu: list[str],
    native_attestation: str,
) -> list[str]:
    errors = []
    if system != "Linux":
        errors.append(f"qualification requires Linux, got {system}")
    if machine != "aarch64":
        errors.append(f"qualification requires uname -m=aarch64, got {machine}")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit.lower()):
        errors.append("expected commit must be a full 40-character SHA-1")
    if commit.lower() != expected_commit.lower():
        errors.append(f"commit mismatch: HEAD={commit}, expected={expected_commit}")
    if status:
        errors.append("qualification requires a clean tracked and untracked worktree")
    if qemu:
        errors.append("QEMU/TCG execution is forbidden: " + "; ".join(qemu))
    if native_attestation != "physical-aarch64":
        errors.append("native runner attestation must be exactly physical-aarch64")
    if not governors:
        errors.append("CPU governor data is unavailable")
    else:
        unexpected = sorted(
            cpu for cpu, governor in governors.items() if governor != expected_governor
        )
        if unexpected:
            errors.append(
                f"CPU governors must all be {expected_governor}; mismatched CPUs: "
                + ",".join(unexpected)
            )
    return errors


def collect_host(repo: Path, expected_governor: str) -> tuple[dict[str, Any], list[str]]:
    commit = run_text(["git", "rev-parse", "HEAD"], repo).lower()
    status = run_text(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], repo
    )
    governors = collect_governors()
    qemu = qemu_indicators()
    cpu_model = ""
    for line in read_text(Path("/proc/cpuinfo")).splitlines():
        if line.lower().startswith(("model name", "hardware")) and ":" in line:
            cpu_model = line.split(":", 1)[1].strip()
            if cpu_model:
                break
    cxx = os.environ.get("CXX", "c++")
    compiler = subprocess.run(
        [cxx, "--version"], check=False, capture_output=True, text=True
    ).stdout.strip()
    memory = ""
    for line in read_text(Path("/proc/meminfo")).splitlines():
        if line.startswith("MemTotal:"):
            memory = line.split(":", 1)[1].strip()
            break
    storage = subprocess.run(
        ["lsblk", "-ndo", "NAME,MODEL,SIZE,ROTA,TYPE"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout.strip()
    filesystem = subprocess.run(
        ["findmnt", "-T", str(repo), "-no", "SOURCE,FSTYPE,OPTIONS"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout.strip()
    host = {
        "system": platform.system(),
        "uname_machine": platform.machine(),
        "kernel": platform.release(),
        "platform": platform.platform(),
        "commit": commit,
        "source_state": "clean" if not status else "dirty",
        "git_status_porcelain": status,
        "qemu_indicators": qemu,
        "cpu_model": cpu_model,
        "logical_cpu_count": os.cpu_count(),
        "memory": memory,
        "storage": storage,
        "filesystem": filesystem,
        "compiler": compiler,
        "cxx": cxx,
        "cpu_governor_policy": {
            "required": expected_governor,
            "observed": governors,
            "all_match": bool(governors)
            and all(value == expected_governor for value in governors.values()),
        },
    }
    return host, qemu


def run_logged(
    name: str,
    command: list[str],
    repo: Path,
    output_dir: Path,
    timeout: int,
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    log_path = output_dir / "logs" / f"{name}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=environment,
        )
    except subprocess.TimeoutExpired as error:
        elapsed = time.monotonic() - started
        stdout = error.stdout or ""
        stderr = error.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        log_path.write_text(
            "$ " + shlex.join(command) + "\n\n[timeout]\n"
            + f"exceeded {timeout} seconds\n\n[stdout]\n" + stdout
            + "\n[stderr]\n" + stderr,
            encoding="utf-8",
        )
        raise QualificationError(
            f"{name} timed out after {elapsed:.3f} seconds; see {log_path}"
        ) from error
    elapsed = time.monotonic() - started
    log_path.write_text(
        "$ " + shlex.join(command) + "\n\n[stdout]\n" + completed.stdout
        + "\n[stderr]\n" + completed.stderr,
        encoding="utf-8",
    )
    result = {
        "name": name,
        "command": command,
        "exit_code": completed.returncode,
        "elapsed_seconds": round(elapsed, 6),
        "log": artifact(log_path, output_dir),
    }
    if completed.returncode != 0:
        raise QualificationError(
            f"{name} failed with exit code {completed.returncode}; see {log_path}"
        )
    return result


def available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def run_bridge_benchmark(
    binary: Path, output_dir: Path, repo: Path, timeout: int
) -> dict[str, Any]:
    port = available_port()
    server_json = output_dir / "benchmarks" / "bridge-server.json"
    client_json = output_dir / "benchmarks" / "bridge-client.json"
    server_log = output_dir / "logs" / "benchmark-bridge-server.log"
    client_log = output_dir / "logs" / "benchmark-bridge-client.log"
    server_log.parent.mkdir(parents=True, exist_ok=True)
    server_json.parent.mkdir(parents=True, exist_ok=True)
    common = [
        "--backend=mino",
        "--layer=l2",
        "--mode=serial",
        "--topic-count=4",
        "--messages-per-topic=1000",
        "--warmup-messages-per-topic=100",
        "--payload-bytes=256",
        "--lane-count=2",
        "--deadline-seconds=30",
        f"--port={port}",
    ]
    server_command = [
        str(binary),
        "--role=server",
        f"--output={server_json}",
        *common,
    ]
    client_command = [
        str(binary),
        "--role=client",
        f"--output={client_json}",
        *common,
    ]
    started = time.monotonic()
    with server_log.open("w", encoding="utf-8") as server_stream:
        server_stream.write("$ " + shlex.join(server_command) + "\n\n")
        server_stream.flush()
        server = subprocess.Popen(
            server_command,
            cwd=repo,
            stdout=server_stream,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            time.sleep(0.5)
            client = subprocess.run(
                client_command,
                cwd=repo,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            client_log.write_text(
                "$ " + shlex.join(client_command) + "\n\n[stdout]\n"
                + client.stdout + "\n[stderr]\n" + client.stderr,
                encoding="utf-8",
            )
            try:
                server_code = server.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                server.terminate()
                server.wait(timeout=5)
                raise QualificationError("bridge benchmark server timed out")
        finally:
            if server.poll() is None:
                server.kill()
                server.wait(timeout=5)
    if client.returncode != 0 or server_code != 0:
        raise QualificationError(
            f"bridge benchmark failed: client={client.returncode}, server={server_code}"
        )
    server_result = json.loads(server_json.read_text(encoding="utf-8"))
    client_result = json.loads(client_json.read_text(encoding="utf-8"))
    if server_result.get("outcome") != "passed" or client_result.get("outcome") != "passed":
        raise QualificationError("bridge benchmark JSON does not report passed")
    return {
        "name": "bridge",
        "commands": [server_command, client_command],
        "exit_codes": {"server": server_code, "client": client.returncode},
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "artifacts": [
            artifact(server_log, output_dir),
            artifact(client_log, output_dir),
            artifact(server_json, output_dir),
            artifact(client_json, output_dir),
        ],
        "result_path": client_json,
    }


def assert_test_log_has_no_skips(path: Path) -> None:
    contents = path.read_text(encoding="utf-8", errors="replace")
    if re.search(r"^\[\s*SKIPPED\s*\]", contents, re.MULTILINE):
        raise QualificationError(f"qualification test reported a skip: {path}")


def parse_allocator(path: Path) -> dict[str, Any]:
    rows = list(csv.DictReader(io.StringIO(path.read_text(encoding="utf-8"))))
    if {row["mode"] for row in rows} != {"legacy_scan", "cursor_cache"}:
        raise QualificationError("allocator benchmark did not produce both modes")
    by_mode = {row["mode"]: row for row in rows}
    legacy = float(by_mode["legacy_scan"]["ops_per_second"])
    cursor = float(by_mode["cursor_cache"]["ops_per_second"])
    return {
        "failures": sum(int(row["failures"]) for row in rows),
        "legacy_ops_per_second": legacy,
        "cursor_cache_ops_per_second": cursor,
        "cursor_cache_vs_legacy_ops_ratio": cursor / legacy if legacy else 0.0,
    }


def parse_channel(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    scenarios = value["results"]["V-14"]["scenarios"]
    maximum = next(item for item in scenarios if item["subscribers"] == 64)
    return {
        "artifact_status": value["artifact_status"],
        "reported_errors": sum(recursive_numeric_values(value["results"], "errors")),
        "subscribers_64_fanout_p99_ns": maximum[
            "fanout_roundtrip_latency_ns"
        ]["p99"],
    }


def parse_bridge(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    records = value["records"]
    return {
        "outcome": value["outcome"],
        "minimum_messages_per_second": min(
            row["messages_per_second"] for row in records
        ),
        "maximum_p99_rtt_us": max(row["p99_rtt_us"] for row in records),
        "sample_count": sum(row["sample_count"] for row in records),
    }


def parse_storage(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    results = value["results"]
    return {
        "reported_errors": sum(recursive_numeric_values(results, "errors")),
        "encode_p99_ns": results["encode_record"]["latency_ns"]["p99"],
        "writer_mebibytes_per_second": results["segment_writer"][
            "write_mebibytes_per_second"
        ],
        "fdatasync_p99_ns": results["segment_writer"]["fdatasync"][
            "latency_ns"
        ]["p99"],
        "recovery_mebibytes_per_second": results["recovery_scan"][
            "mebibytes_per_second"
        ],
        "buffer_records_per_second": results["buffer_mpsc"][
            "records_per_second"
        ],
    }


def parse_telemetry(path: Path) -> dict[str, Any]:
    pattern = re.compile(
        r"^(baseline|off|counters|sampled-1pct|full)\s+"
        r"([0-9.]+)\s+(-?[0-9.]+)%\s+(\d+)\s+(\d+)\s+(\d+)$"
    )
    rows: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line.strip())
        if match:
            rows[match.group(1)] = {
                "ns_per_operation": float(match.group(2)),
                "overhead_percent": float(match.group(3)),
                "counter": int(match.group(4)),
                "samples": int(match.group(5)),
                "dropped": int(match.group(6)),
            }
    if set(rows) != {"baseline", "off", "counters", "sampled-1pct", "full"}:
        raise QualificationError("telemetry benchmark output is incomplete")
    return {
        "modes": rows,
        "dropped": sum(row["dropped"] for row in rows.values()),
        "counters_overhead_percent": rows["counters"]["overhead_percent"],
        "sampled_1pct_overhead_percent": rows["sampled-1pct"][
            "overhead_percent"
        ],
    }


def check(condition: bool, metric: str, observed: Any, threshold: Any) -> dict[str, Any]:
    return {
        "metric": metric,
        "observed": observed,
        "threshold": threshold,
        "passed": bool(condition),
    }


def evaluate_sla(
    measurements: dict[str, Any], policy: dict[str, Any]
) -> dict[str, Any]:
    arm = policy["aarch64"]
    allocator = measurements["allocator"]
    channel = measurements["channel"]
    bridge = measurements["bridge"]
    storage = measurements["storage"]
    telemetry = measurements["telemetry"]
    checks = [
        check(allocator["failures"] <= arm["allocator"]["max_failures"],
              "allocator.failures", allocator["failures"], arm["allocator"]["max_failures"]),
        check(allocator["cursor_cache_vs_legacy_ops_ratio"] >= arm["allocator"]["min_cursor_cache_vs_legacy_ops_ratio"],
              "allocator.cursor_cache_vs_legacy_ops_ratio", allocator["cursor_cache_vs_legacy_ops_ratio"], arm["allocator"]["min_cursor_cache_vs_legacy_ops_ratio"]),
        check(channel["artifact_status"] == arm["channel"]["required_artifact_status"],
              "channel.artifact_status", channel["artifact_status"], arm["channel"]["required_artifact_status"]),
        check(channel["reported_errors"] <= arm["channel"]["max_reported_errors"],
              "channel.reported_errors", channel["reported_errors"], arm["channel"]["max_reported_errors"]),
        check(channel["subscribers_64_fanout_p99_ns"] <= arm["channel"]["max_64_subscriber_fanout_p99_ns"],
              "channel.subscribers_64_fanout_p99_ns", channel["subscribers_64_fanout_p99_ns"], arm["channel"]["max_64_subscriber_fanout_p99_ns"]),
        check(bridge["outcome"] == arm["bridge"]["required_outcome"],
              "bridge.outcome", bridge["outcome"], arm["bridge"]["required_outcome"]),
        check(bridge["minimum_messages_per_second"] >= arm["bridge"]["min_messages_per_second"],
              "bridge.minimum_messages_per_second", bridge["minimum_messages_per_second"], arm["bridge"]["min_messages_per_second"]),
        check(bridge["maximum_p99_rtt_us"] <= arm["bridge"]["max_p99_rtt_us"],
              "bridge.maximum_p99_rtt_us", bridge["maximum_p99_rtt_us"], arm["bridge"]["max_p99_rtt_us"]),
        check(storage["reported_errors"] <= arm["storage_1k_per_batch"]["max_reported_errors"],
              "storage.reported_errors", storage["reported_errors"], arm["storage_1k_per_batch"]["max_reported_errors"]),
        check(storage["writer_mebibytes_per_second"] >= arm["storage_1k_per_batch"]["min_writer_mebibytes_per_second"],
              "storage.writer_mebibytes_per_second", storage["writer_mebibytes_per_second"], arm["storage_1k_per_batch"]["min_writer_mebibytes_per_second"]),
        check(storage["fdatasync_p99_ns"] <= arm["storage_1k_per_batch"]["max_fdatasync_p99_ns"],
              "storage.fdatasync_p99_ns", storage["fdatasync_p99_ns"], arm["storage_1k_per_batch"]["max_fdatasync_p99_ns"]),
        check(storage["recovery_mebibytes_per_second"] >= arm["storage_1k_per_batch"]["min_recovery_mebibytes_per_second"],
              "storage.recovery_mebibytes_per_second", storage["recovery_mebibytes_per_second"], arm["storage_1k_per_batch"]["min_recovery_mebibytes_per_second"]),
        check(storage["buffer_records_per_second"] >= arm["storage_1k_per_batch"]["min_buffer_records_per_second"],
              "storage.buffer_records_per_second", storage["buffer_records_per_second"], arm["storage_1k_per_batch"]["min_buffer_records_per_second"]),
        check(telemetry["dropped"] <= arm["telemetry"]["max_dropped"],
              "telemetry.dropped", telemetry["dropped"], arm["telemetry"]["max_dropped"]),
        check(telemetry["counters_overhead_percent"] <= arm["telemetry"]["max_counters_overhead_percent"],
              "telemetry.counters_overhead_percent", telemetry["counters_overhead_percent"], arm["telemetry"]["max_counters_overhead_percent"]),
        check(telemetry["sampled_1pct_overhead_percent"] <= arm["telemetry"]["max_sampled_1pct_overhead_percent"],
              "telemetry.sampled_1pct_overhead_percent", telemetry["sampled_1pct_overhead_percent"], arm["telemetry"]["max_sampled_1pct_overhead_percent"]),
    ]
    x86 = policy["x86_reference"]
    x86_storage = x86["storage_1k_per_batch"]
    x86_storage_checks = [
        check(storage["encode_p99_ns"] <= x86_storage["max_encode_p99_ns"],
              "storage.encode_p99_ns", storage["encode_p99_ns"], x86_storage["max_encode_p99_ns"]),
        check(storage["writer_mebibytes_per_second"] >= x86_storage["min_writer_mebibytes_per_second"],
              "storage.writer_mebibytes_per_second", storage["writer_mebibytes_per_second"], x86_storage["min_writer_mebibytes_per_second"]),
        check(storage["fdatasync_p99_ns"] <= x86_storage["max_fdatasync_p99_ns"],
              "storage.fdatasync_p99_ns", storage["fdatasync_p99_ns"], x86_storage["max_fdatasync_p99_ns"]),
        check(storage["recovery_mebibytes_per_second"] >= x86_storage["min_recovery_mebibytes_per_second"],
              "storage.recovery_mebibytes_per_second", storage["recovery_mebibytes_per_second"], x86_storage["min_recovery_mebibytes_per_second"]),
        check(storage["buffer_records_per_second"] >= x86_storage["min_buffer_records_per_second"],
              "storage.buffer_records_per_second", storage["buffer_records_per_second"], x86_storage["min_buffer_records_per_second"]),
    ]
    x86_comparison = {
        "policy": x86,
        "storage_same_workload": {
            "checks": x86_storage_checks,
            "would_meet_published_x86_sla": all(
                item["passed"] for item in x86_storage_checks
            ),
            "note": "informational comparison only; AArch64 qualification uses its independent thresholds",
        },
    }
    return {
        "architecture": "aarch64",
        "passed": all(item["passed"] for item in checks),
        "checks": checks,
        "x86_reference_comparison": x86_comparison,
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-governor", default="performance")
    parser.add_argument("--native-attestation", required=True)
    parser.add_argument("--bazel-config", default="gcc12")
    parser.add_argument("--sla", type=Path, default=Path("tests/aarch64/benchmark_sla.json"))
    parser.add_argument("--test-timeout", type=int, default=1800)
    parser.add_argument("--benchmark-timeout", type=int, default=900)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    repo = Path(__file__).resolve().parents[2]
    output_dir = args.output_dir.resolve()
    manifest_path = output_dir / "manifest.json"
    manifest: dict[str, Any] = {
        "schema": SCHEMA,
        "qualification": "V-13",
        "architecture": "aarch64",
        "qualification_eligible": False,
        "outcome": "failed",
        "expected_commit": args.expected_commit.lower(),
        "commands": [],
        "tests": [],
        "benchmarks": [],
        "artifacts_complete": False,
        "failure": None,
    }
    try:
        try:
            output_dir.relative_to(repo)
            raise QualificationError(
                "qualification output directory must be outside the git worktree"
            )
        except ValueError:
            pass
        host, qemu = collect_host(repo, args.expected_governor)
        manifest["host"] = host
        preflight_errors = evaluate_preflight(
            system=host["system"],
            machine=host["uname_machine"],
            commit=host["commit"],
            expected_commit=args.expected_commit,
            status=host["git_status_porcelain"],
            governors=host["cpu_governor_policy"]["observed"],
            expected_governor=args.expected_governor,
            qemu=qemu,
            native_attestation=args.native_attestation,
        )
        manifest["preflight"] = {
            "passed": not preflight_errors,
            "errors": preflight_errors,
            "native_attestation": args.native_attestation,
        }
        if preflight_errors:
            raise QualificationError("; ".join(preflight_errors))

        output_dir.mkdir(parents=True, exist_ok=False)
        environment = os.environ.copy()
        environment["LC_ALL"] = "C"
        common = [
            "--lockfile_mode=error",
            f"--config={args.bazel_config}",
            "--config=release",
        ]
        for group, label in TEST_GROUPS.items():
            result = run_logged(
                f"test-{group}",
                [
                    "bazel",
                    "test",
                    *common,
                    label,
                    "--test_output=all",
                    "--nocache_test_results",
                ],
                repo,
                output_dir,
                args.test_timeout,
                environment,
            )
            result["group"] = group
            result["label"] = label
            assert_test_log_has_no_skips(output_dir / result["log"]["path"])
            result["skipped_tests"] = 0
            manifest["tests"].append(result)

        manifest["commands"].append(
            run_logged(
                "build-release-benchmarks",
                ["bazel", "build", *common, *BENCHMARK_TARGETS],
                repo,
                output_dir,
                args.test_timeout,
                environment,
            )
        )
        benchmark_dir = output_dir / "benchmarks"
        benchmark_dir.mkdir(parents=True, exist_ok=True)
        bazel_bin = repo / "bazel-bin"

        allocator_log = output_dir / "logs" / "benchmark-allocator.log"
        allocator_command = [
            str(bazel_bin / "benchmarks/allocator/allocator_benchmark"),
            "--iterations=100000",
            "--slots=4096",
        ]
        allocator_run = run_logged(
            "benchmark-allocator", allocator_command, repo, output_dir,
            args.benchmark_timeout, environment
        )
        allocator_csv = benchmark_dir / "allocator.csv"
        allocator_text = allocator_log.read_text(encoding="utf-8")
        stdout = allocator_text.split("[stdout]\n", 1)[1].split("\n[stderr]", 1)[0]
        allocator_csv.write_text(stdout.strip() + "\n", encoding="utf-8")
        allocator_run["name"] = "allocator"
        allocator_run["result"] = artifact(allocator_csv, output_dir)
        manifest["benchmarks"].append(allocator_run)

        channel_json = benchmark_dir / "channel-validation.json"
        channel_command = [
            str(bazel_bin / "benchmarks/validation_benchmark"),
            "--suite=memory",
            "--iterations=10000",
            "--pin-count=1000",
            f"--commit={host['commit']}",
            f"--build-config=bazel --config={args.bazel_config} --config=release",
            f"--cpu-model={host['cpu_model']}",
            f"--memory={host['memory']}",
            f"--storage-device={host['storage']}",
            f"--filesystem={host['filesystem']}",
            f"--output-json={channel_json}",
        ]
        channel_run = run_logged(
            "benchmark-channel", channel_command, repo, output_dir,
            args.benchmark_timeout, environment
        )
        channel_run["name"] = "channel"
        channel_run["result"] = artifact(channel_json, output_dir)
        manifest["benchmarks"].append(channel_run)

        bridge_run = run_bridge_benchmark(
            bazel_bin / "benchmarks/transport/layer_comparison_benchmark",
            output_dir,
            repo,
            args.benchmark_timeout,
        )
        bridge_result_path = bridge_run.pop("result_path")
        manifest["benchmarks"].append(bridge_run)

        storage_json = benchmark_dir / "storage-1k.json"
        storage_command = [
            str(bazel_bin / "benchmarks/storage_benchmark"),
            "--records=20000",
            "--payload-bytes=1024",
            "--sync-policy=per-batch",
            f"--directory={output_dir}",
            f"--output-json={storage_json}",
        ]
        storage_run = run_logged(
            "benchmark-storage", storage_command, repo, output_dir,
            args.benchmark_timeout, environment
        )
        storage_run["name"] = "storage"
        storage_run["result"] = artifact(storage_json, output_dir)
        manifest["benchmarks"].append(storage_run)

        telemetry_run = run_logged(
            "benchmark-telemetry",
            [str(bazel_bin / "mino/observability/v23_telemetry_benchmark"), "5000000"],
            repo,
            output_dir,
            args.benchmark_timeout,
            environment,
        )
        telemetry_run["name"] = "telemetry"
        manifest["benchmarks"].append(telemetry_run)
        telemetry_log = output_dir / telemetry_run["log"]["path"]

        sla_path = args.sla if args.sla.is_absolute() else repo / args.sla
        policy = json.loads(sla_path.read_text(encoding="utf-8"))
        if policy.get("schema") != "mino.aarch64.benchmark_sla.v1":
            raise QualificationError("unexpected AArch64 SLA schema")
        measurements = {
            "allocator": parse_allocator(allocator_csv),
            "channel": parse_channel(channel_json),
            "bridge": parse_bridge(bridge_result_path),
            "storage": parse_storage(storage_json),
            "telemetry": parse_telemetry(telemetry_log),
        }
        manifest["benchmark_measurements"] = measurements
        manifest["sla"] = evaluate_sla(measurements, policy)
        manifest["sla_policy"] = {
            "source": str(sla_path.relative_to(repo)),
            "sha256": sha256(sla_path),
        }
        if not manifest["sla"]["passed"]:
            failed = [
                item["metric"] for item in manifest["sla"]["checks"]
                if not item["passed"]
            ]
            raise QualificationError("AArch64 SLA failed: " + ", ".join(failed))

        expected_benchmarks = {"allocator", "channel", "bridge", "storage", "telemetry"}
        observed_benchmarks = {item["name"] for item in manifest["benchmarks"]}
        if len(manifest["tests"]) != len(TEST_GROUPS):
            raise QualificationError("test matrix is incomplete")
        if observed_benchmarks != expected_benchmarks:
            raise QualificationError("benchmark matrix is incomplete")
        manifest["artifacts_complete"] = True
        manifest["qualification_eligible"] = True
        manifest["outcome"] = "passed"
        return_code = 0
    except (QualificationError, OSError, subprocess.TimeoutExpired, json.JSONDecodeError, KeyError, ValueError) as error:
        manifest["failure"] = str(error)
        return_code = 1
    finally:
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        write_json(manifest_path, manifest)
        print(f"AArch64 qualification {manifest['outcome']}: {manifest_path}")
        if manifest["failure"]:
            print(manifest["failure"], file=sys.stderr)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
