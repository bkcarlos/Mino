#!/usr/bin/env python3
"""Fail-closed D6-09/V-24 storage partition qualification runner."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import statistics
import subprocess
import sys
import time
from typing import Any

ARTIFACT_SCHEMA = "mino.storage_partition_qualification.v1"
POLICY_SCHEMA = "mino.storage_partition_qualification_sla.v1"
BENCHMARK_SCHEMA = "mino.storage_partition_benchmark.v1"
REQUIRED_PARTITIONS = [1, 2, 4, 8, 16]
STABLE_HASH_VERSION = 1
STABLE_HASH_SEED = 0x6D696E6F2D703031
ATTESTATION = "auto-probed-local-block-device"


class QualificationError(RuntimeError):
    pass


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


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def artifact(path: Path, root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root.resolve())
    except ValueError as error:
        raise QualificationError(f"artifact escapes evidence root: {path}") from error
    if not resolved.is_file() or resolved.stat().st_size == 0:
        raise QualificationError(f"required artifact missing or empty: {path}")
    return {
        "path": relative.as_posix(),
        "bytes": resolved.stat().st_size,
        "sha256": sha256(resolved),
    }


def verify_artifact(record: Any, root: Path) -> list[str]:
    if not isinstance(record, dict):
        return ["artifact record is malformed"]
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        return ["artifact path is invalid"]
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return [f"artifact path escapes evidence root: {relative}"]
    if not path.is_file():
        return [f"artifact is missing: {relative}"]
    expected_bytes = record.get("bytes")
    expected_hash = record.get("sha256")
    if (
        not isinstance(expected_bytes, int)
        or isinstance(expected_bytes, bool)
        or expected_bytes <= 0
        or not isinstance(expected_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
    ):
        return [f"artifact metadata is invalid: {relative}"]
    if path.stat().st_size != expected_bytes or sha256(path) != expected_hash:
        return [f"artifact size or SHA-256 mismatch: {relative}"]
    return []


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


def git_source(repo: Path, expected_commit: str) -> tuple[dict[str, str], list[str]]:
    errors: list[str] = []
    commit = ""
    state = "unknown"
    try:
        commit = run_text(["git", "rev-parse", "HEAD"], repo).lower()
        status = run_text(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"], repo
        )
        state = "clean" if not status else "dirty"
        if commit != expected_commit.lower():
            errors.append(f"HEAD {commit} does not match exact expected commit {expected_commit.lower()}")
        if status:
            errors.append("qualification rejects a dirty worktree, including untracked files")
    except (OSError, subprocess.SubprocessError, QualificationError) as error:
        errors.append(str(error))
    return {"commit": commit, "state": state}, errors


def collect_governors(root: Path = Path("/sys/devices/system/cpu")) -> dict[str, str]:
    governors: dict[str, str] = {}
    for path in sorted(root.glob("cpu[0-9]*/cpufreq/scaling_governor")):
        value = read_text(path)
        if value:
            governors[path.parent.parent.name] = value
    return governors


def collect_cpu_model() -> str:
    for line in read_text(Path("/proc/cpuinfo")).splitlines():
        if line.lower().startswith("model name") and ":" in line:
            return line.split(":", 1)[1].strip()
    return platform.processor().strip()


def collect_mount(directory: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "target": "",
        "source": "",
        "filesystem": "",
        "mount_options": [],
        "probe_error": "",
    }
    try:
        output = run_text(
            [
                "findmnt",
                "--json",
                "--target",
                str(directory),
                "--output",
                "SOURCE,TARGET,FSTYPE,OPTIONS",
            ],
            directory,
        )
        payload = json.loads(output)
        rows = payload.get("filesystems")
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise QualificationError("findmnt did not return exactly one filesystem")
        row = rows[0]
        evidence = {
            "target": str(row.get("target", "")),
            "source": str(row.get("source", "")),
            "filesystem": str(row.get("fstype", "")),
            "mount_options": sorted(
                option for option in str(row.get("options", "")).split(",") if option
            ),
            "probe_error": "",
        }
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, QualificationError) as error:
        evidence["probe_error"] = str(error)
    return evidence


def _flatten_block_devices(rows: Any) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    if not isinstance(rows, list):
        return flattened
    for row in rows:
        if not isinstance(row, dict):
            continue
        flattened.append(row)
        flattened.extend(_flatten_block_devices(row.get("children")))
    return flattened


def collect_storage(mount: dict[str, Any], cwd: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "mount_source": mount.get("source", ""),
        "device": "",
        "parent_device": "",
        "type": "",
        "model": "",
        "serial": "",
        "transport": "",
        "rotational": None,
        "probe_error": "",
    }
    source = str(mount.get("source", "")).split("[", 1)[0]
    if not source.startswith("/dev/"):
        evidence["probe_error"] = "mount source is not a local /dev block device"
        return evidence
    try:
        output = run_text(
            [
                "lsblk",
                "--json",
                "--paths",
                "--output",
                "NAME,PKNAME,TYPE,MODEL,SERIAL,TRAN,ROTA",
            ],
            cwd,
        )
        payload = json.loads(output)
        rows = _flatten_block_devices(payload.get("blockdevices"))
        by_name = {str(row.get("name", "")): row for row in rows}
        current = by_name.get(source)
        if current is None:
            raise QualificationError(f"lsblk has no row for mount source {source}")
        leaf = current
        seen: set[str] = set()
        while str(current.get("pkname", "")):
            parent_name = str(current.get("pkname"))
            if parent_name in seen or parent_name not in by_name:
                raise QualificationError("lsblk parent chain is incomplete or cyclic")
            seen.add(parent_name)
            current = by_name[parent_name]
        rotational = current.get("rota")
        evidence = {
            "mount_source": source,
            "device": str(leaf.get("name", "")),
            "parent_device": str(current.get("name", "")),
            "type": str(current.get("type", "")),
            "model": str(current.get("model", "")).strip(),
            "serial": str(current.get("serial", "")).strip(),
            "transport": str(current.get("tran", "")).strip().lower(),
            "rotational": rotational in (True, 1, "1"),
            "probe_error": "",
        }
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, QualificationError) as error:
        evidence["probe_error"] = str(error)
    return evidence


def collect_host() -> dict[str, Any]:
    return {
        "system": platform.system(),
        "machine": platform.machine().lower(),
        "kernel": platform.release(),
        "cpu_model": collect_cpu_model(),
        "cpu_governors": collect_governors(),
    }


def validate_attestation(value: str) -> list[str]:
    if value != ATTESTATION:
        return [
            f"hardware attestation must be exactly {ATTESTATION!r}; claimed/fake hardware is rejected"
        ]
    return []


def evaluate_target(
    host: dict[str, Any], mount: dict[str, Any], storage: dict[str, Any], target: dict[str, Any]
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if host.get("system") != target.get("system"):
        reasons.append(f"system is {host.get('system')!r}, expected {target.get('system')!r}")
    machines = target.get("machines", [])
    if host.get("machine") not in machines:
        reasons.append(f"machine is {host.get('machine')!r}, expected one of {machines!r}")
    try:
        if re.search(str(target.get("cpu_model_regex", "")), str(host.get("cpu_model", ""))) is None:
            reasons.append("CPU model does not match the target policy")
        if re.search(str(target.get("storage_model_regex", "")), str(storage.get("model", ""))) is None:
            reasons.append("storage model does not match the target policy")
    except re.error as error:
        reasons.append(f"target policy contains an invalid model regex: {error}")
    governors = host.get("cpu_governors")
    expected_governor = target.get("cpu_governor")
    if not isinstance(governors, dict) or not governors:
        reasons.append("CPU governor provenance is unavailable")
    elif any(value != expected_governor for value in governors.values()):
        reasons.append(f"not all CPU governors are {expected_governor!r}")
    if mount.get("probe_error"):
        reasons.append(f"filesystem probe failed: {mount['probe_error']}")
    if mount.get("filesystem") != target.get("filesystem"):
        reasons.append(
            f"filesystem is {mount.get('filesystem')!r}, expected {target.get('filesystem')!r}"
        )
    options = set(mount.get("mount_options", []))
    for option in target.get("required_mount_options", []):
        if option not in options:
            reasons.append(f"required mount option is absent: {option}")
    for option in target.get("forbidden_mount_options", []):
        if option in options:
            reasons.append(f"forbidden mount option is present: {option}")
    if storage.get("probe_error"):
        reasons.append(f"storage probe failed: {storage['probe_error']}")
    if storage.get("transport") != target.get("storage_transport"):
        reasons.append(
            f"storage transport is {storage.get('transport')!r}, expected {target.get('storage_transport')!r}"
        )
    if storage.get("rotational") is not target.get("rotational"):
        reasons.append("storage rotational flag does not match target policy")
    if storage.get("type") != "disk":
        reasons.append("resolved storage parent is not a physical disk")
    return not reasons, reasons


def load_policy(path: Path) -> dict[str, Any]:
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot load SLA policy: {error}") from error
    if policy.get("schema") != POLICY_SCHEMA:
        raise QualificationError("SLA policy schema mismatch")
    if policy.get("artifact_schema") != ARTIFACT_SCHEMA:
        raise QualificationError("SLA policy artifact schema mismatch")
    if policy.get("benchmark_schema") != BENCHMARK_SCHEMA:
        raise QualificationError("SLA policy benchmark schema mismatch")
    if policy.get("qualification_id") != "D6-09" or policy.get("validation") != "V-24":
        raise QualificationError("SLA policy qualification identity mismatch")
    if policy.get("required_partition_counts") != REQUIRED_PARTITIONS:
        raise QualificationError("SLA policy partition matrix mismatch")
    workload = policy.get("workload")
    target = policy.get("target")
    limits = policy.get("validation_limits")
    sla = policy.get("sla")
    if not isinstance(workload, dict) or any(
        not _positive_int(workload.get(key))
        for key in (
            "records_per_scenario",
            "payload_bytes",
            "target_ingress_records_per_second",
        )
    ):
        raise QualificationError("SLA policy workload is incomplete")
    required_target_keys = {
        "hardware_label",
        "system",
        "machines",
        "cpu_model_regex",
        "cpu_governor",
        "storage_model_regex",
        "storage_transport",
        "rotational",
        "filesystem",
        "required_mount_options",
        "forbidden_mount_options",
    }
    if not isinstance(target, dict) or not required_target_keys.issubset(target):
        raise QualificationError("SLA policy target is incomplete")
    if not isinstance(limits, dict) or not _positive_number(
        limits.get("maximum_partition_throughput_imbalance_ratio")
    ) or not _positive_int(limits.get("maximum_record_latency_p99_ns")):
        raise QualificationError("SLA policy validation limits are incomplete")
    expected_efficiency_keys = {str(value) for value in REQUIRED_PARTITIONS}
    if (
        not isinstance(sla, dict)
        or not _positive_number(
            sla.get("minimum_single_writer_threshold_records_per_second")
        )
        or not isinstance(sla.get("minimum_scaling_efficiency"), dict)
        or set(sla["minimum_scaling_efficiency"]) != expected_efficiency_keys
        or any(
            not _positive_number(value)
            for value in sla["minimum_scaling_efficiency"].values()
        )
        or sla.get("aggregation")
        != "minimum_across_all_processes_and_rounds"
    ):
        raise QualificationError("SLA policy thresholds/aggregation are incomplete")
    return policy


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _zero_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value == 0


def _positive_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0.0
    )


def _close(left: Any, right: Any) -> bool:
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-6, abs_tol=1e-9)
    except (TypeError, ValueError):
        return False


def validate_benchmark_report(
    report: Any,
    policy: dict[str, Any],
    target_match: bool,
) -> list[str]:
    errors: list[str] = []
    if not isinstance(report, dict):
        return ["benchmark JSON root is not an object"]
    if report.get("schema") != BENCHMARK_SCHEMA or report.get("validation") != "V-24":
        errors.append("benchmark JSON schema/validation identity mismatch")
    workload = policy["workload"]
    expected_config = {
        "records": workload["records_per_scenario"],
        "payload_bytes": workload["payload_bytes"],
        "partition_counts": REQUIRED_PARTITIONS,
    }
    if report.get("configuration") != expected_config:
        errors.append("benchmark workload configuration mismatch")
    if not _zero_int(report.get("errors")):
        errors.append("benchmark errors must be exactly integer zero")
    qualification = report.get("qualification")
    if not isinstance(qualification, dict):
        errors.append("benchmark qualification object is missing")
    else:
        expected_label = policy["target"]["hardware_label"]
        if qualification.get("required_target") != expected_label:
            errors.append("benchmark required hardware target differs from policy")
        expected_claim = expected_label if target_match else ""
        if qualification.get("target") != expected_claim:
            errors.append("benchmark hardware target was not derived from runner probes")
        if qualification.get("eligible") is not target_match:
            errors.append("benchmark hardware eligibility disagrees with runner probes")
    if report.get("target_ingress_records_per_second") != workload["target_ingress_records_per_second"]:
        errors.append("benchmark target ingress differs from policy")
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list):
        return errors + ["benchmark scenarios are missing"]
    counts = [scenario.get("partitions") for scenario in scenarios if isinstance(scenario, dict)]
    if (
        counts != REQUIRED_PARTITIONS
        or any(type(value) is not int for value in counts)
        or len(scenarios) != len(REQUIRED_PARTITIONS)
    ):
        errors.append("benchmark must contain ordered, exact 1/2/4/8/16 partition scenarios")
        return errors
    baseline = scenarios[0].get("records_per_second")
    if not _positive_number(baseline):
        errors.append("single-writer throughput must be positive and finite")
        baseline = 0.0
    baseline_for_math = float(baseline) if float(baseline) > 0.0 else 1.0
    if not _close(report.get("single_writer_threshold_records_per_second"), baseline):
        errors.append("single-writer threshold does not equal the one-partition throughput")
    expected_partitioning = workload["target_ingress_records_per_second"] > float(baseline)
    if report.get("partitioning_required_for_target") is not expected_partitioning:
        errors.append("partitioning_required_for_target is inconsistent")

    max_imbalance = float(policy["validation_limits"]["maximum_partition_throughput_imbalance_ratio"])
    max_p99 = int(policy["validation_limits"]["maximum_record_latency_p99_ns"])
    expected_records = int(workload["records_per_scenario"])
    for scenario in scenarios:
        partitions = scenario["partitions"]
        prefix = f"partitions={partitions}"
        conservation_fields = (
            "records",
            "attempted_records",
            "accepted_records",
            "dequeued_records",
            "written_records",
        )
        if any(scenario.get(field) != expected_records for field in conservation_fields):
            errors.append(f"{prefix}: aggregate record conservation failed")
        if not _zero_int(scenario.get("errors")):
            errors.append(f"{prefix}: errors must be exactly integer zero")
        stable_map = scenario.get("stable_partition_map")
        expected_map = {
            "map_version": 1,
            "generation": 1,
            "partition_count": partitions,
            "strategy": "hash",
            "state": "active",
            "hash_algorithm_version": STABLE_HASH_VERSION,
            "hash_seed": STABLE_HASH_SEED,
        }
        if stable_map != expected_map or not isinstance(stable_map, dict) or any(
            type(stable_map.get(key)) is not type(value)
            for key, value in expected_map.items()
        ):
            errors.append(f"{prefix}: stable partition map identity/type mismatch")
        if not _positive_int(scenario.get("elapsed_ns")) or not _positive_number(
            scenario.get("records_per_second")
        ):
            errors.append(f"{prefix}: elapsed time/throughput must be positive")
        elif not _close(
            scenario["records_per_second"], expected_records * 1e9 / scenario["elapsed_ns"]
        ):
            errors.append(f"{prefix}: aggregate throughput is internally inconsistent")
        p50 = scenario.get("record_latency_p50_ns")
        p99 = scenario.get("record_latency_p99_ns")
        if not _positive_int(p50) or not _positive_int(p99) or p99 < p50:
            errors.append(f"{prefix}: p50/p99 latency is incomplete or unordered")
        elif p99 > max_p99:
            errors.append(f"{prefix}: p99 latency exceeds validation limit {max_p99}")
        scaling = scenario.get("scaling")
        efficiency = scenario.get("scaling_efficiency")
        observed_throughput = scenario.get("records_per_second")
        expected_scaling = (
            float(observed_throughput) / baseline_for_math
            if _positive_number(observed_throughput)
            else 0.0
        )
        if not _positive_number(scaling) or not _close(scaling, expected_scaling):
            errors.append(f"{prefix}: scaling is invalid or inconsistent")
        if not _positive_number(efficiency) or not _close(efficiency, expected_scaling / partitions):
            errors.append(f"{prefix}: scaling efficiency is invalid or inconsistent")
        partition_results = scenario.get("partition_results")
        if not isinstance(partition_results, list) or len(partition_results) != partitions:
            errors.append(f"{prefix}: per-partition results are incomplete")
            continue
        ids = [row.get("partition_id") for row in partition_results if isinstance(row, dict)]
        if ids != list(range(partitions)):
            errors.append(f"{prefix}: partition IDs are missing, duplicated, or unordered")
            continue
        partition_rates: list[float] = []
        sums = {field: 0 for field in conservation_fields[1:]}
        for row in partition_results:
            partition_id = row["partition_id"]
            expected_partition_records = expected_records // partitions + (
                1 if partition_id < expected_records % partitions else 0
            )
            for field in conservation_fields[1:]:
                value = row.get(field)
                if value != expected_partition_records:
                    errors.append(f"{prefix}/partition={partition_id}: {field} violates conservation")
                if _nonnegative_int(value):
                    sums[field] += value
            if not _zero_int(row.get("errors")):
                errors.append(
                    f"{prefix}/partition={partition_id}: errors must be integer zero"
                )
            if not _positive_int(row.get("elapsed_ns")) or not _positive_number(
                row.get("records_per_second")
            ):
                errors.append(f"{prefix}/partition={partition_id}: throughput evidence is invalid")
                continue
            expected_rate = expected_partition_records * 1e9 / row["elapsed_ns"]
            if not _close(row["records_per_second"], expected_rate):
                errors.append(f"{prefix}/partition={partition_id}: throughput is inconsistent")
            partition_rates.append(float(row["records_per_second"]))
        if any(total != expected_records for total in sums.values()):
            errors.append(f"{prefix}: per-partition record sums violate conservation")
        imbalance = scenario.get("partition_throughput_imbalance_ratio")
        if len(partition_rates) == partitions:
            expected_imbalance = max(partition_rates) / min(partition_rates)
            if not _positive_number(imbalance) or not _close(imbalance, expected_imbalance):
                errors.append(f"{prefix}: throughput imbalance is invalid or inconsistent")
            elif float(imbalance) > max_imbalance:
                errors.append(f"{prefix}: throughput imbalance exceeds validation limit {max_imbalance}")
    return errors


def evaluate_sla(reports: list[dict[str, Any]], policy: dict[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    threshold_values = [
        float(report["single_writer_threshold_records_per_second"]) for report in reports
    ]
    threshold_policy = float(policy["sla"]["minimum_single_writer_threshold_records_per_second"])
    observed_threshold = min(threshold_values) if threshold_values else 0.0
    checks.append(
        {
            "metric": "single_writer_threshold_records_per_second",
            "partitions": 1,
            "aggregation": "minimum",
            "observed": observed_threshold,
            "threshold": threshold_policy,
            "policy": ">=",
            "passed": observed_threshold >= threshold_policy,
        }
    )
    efficiency_policy = policy["sla"]["minimum_scaling_efficiency"]
    observations: list[dict[str, Any]] = []
    for partitions in REQUIRED_PARTITIONS:
        scenarios = [
            next(item for item in report["scenarios"] if item["partitions"] == partitions)
            for report in reports
        ]
        throughputs = [float(item["records_per_second"]) for item in scenarios]
        efficiencies = [float(item["scaling_efficiency"]) for item in scenarios]
        imbalances = [float(item["partition_throughput_imbalance_ratio"]) for item in scenarios]
        p99_values = [int(item["record_latency_p99_ns"]) for item in scenarios]
        minimum_efficiency = min(efficiencies) if efficiencies else 0.0
        efficiency_threshold = float(efficiency_policy[str(partitions)])
        checks.append(
            {
                "metric": "scaling_efficiency",
                "partitions": partitions,
                "aggregation": "minimum",
                "observed": minimum_efficiency,
                "threshold": efficiency_threshold,
                "policy": ">=",
                "passed": minimum_efficiency >= efficiency_threshold,
            }
        )
        observations.append(
            {
                "partitions": partitions,
                "throughput_records_per_second": {
                    "minimum": min(throughputs),
                    "median": statistics.median(throughputs),
                    "maximum": max(throughputs),
                },
                "minimum_scaling_efficiency": minimum_efficiency,
                "maximum_partition_throughput_imbalance_ratio": max(imbalances),
                "maximum_record_latency_p99_ns": max(p99_values),
            }
        )
    return {
        "aggregation": policy["sla"]["aggregation"],
        "checks": checks,
        "observations": observations,
        "passed": bool(reports) and all(check["passed"] for check in checks),
    }


def derive_outcome(
    errors: list[str], artifacts_complete: bool, target_match: bool, sla_passed: bool
) -> tuple[str, bool]:
    if errors or not artifacts_complete or not sla_passed:
        return "failed", False
    if not target_match:
        return "nonqualified", False
    return "passed", True


def _execute_process(
    *,
    benchmark: Path,
    repo: Path,
    evidence_root: Path,
    storage_dir: Path,
    round_number: int,
    process_number: int,
    workload: dict[str, Any],
    qualification_target: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    run_dir = evidence_root / "runs" / f"round-{round_number:02d}" / f"process-{process_number:02d}"
    run_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_dir / "benchmark.json"
    log_path = run_dir / "benchmark.log"
    command = [
        str(benchmark),
        f"--records={workload['records_per_scenario']}",
        f"--payload-bytes={workload['payload_bytes']}",
        f"--target-ingress-rps={workload['target_ingress_records_per_second']}",
        f"--qualification-hardware={qualification_target}",
        f"--directory={storage_dir}",
        f"--output-json={json_path}",
    ]
    started_ns = time.time_ns()
    stdout = ""
    stderr = ""
    exit_code = -1
    execution_error = ""
    try:
        completed = subprocess.run(
            command,
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        stdout = completed.stdout
        stderr = completed.stderr
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout.decode("utf-8", "replace") if isinstance(error.stdout, bytes) else (error.stdout or "")
        stderr = error.stderr.decode("utf-8", "replace") if isinstance(error.stderr, bytes) else (error.stderr or "")
        execution_error = f"benchmark timed out after {timeout_seconds} seconds"
    except OSError as error:
        execution_error = f"cannot execute benchmark: {error}"
    finished_ns = time.time_ns()
    log_path.write_text(
        "$ "
        + shlex.join(command)
        + f"\nstarted_unix_ns={started_ns}\nfinished_unix_ns={finished_ns}\nexit_code={exit_code}\n"
        + (f"execution_error={execution_error}\n" if execution_error else "")
        + "\nSTDOUT\n"
        + stdout
        + "\nSTDERR\n"
        + stderr,
        encoding="utf-8",
    )
    result: dict[str, Any] = {
        "round": round_number,
        "process": process_number,
        "command": command,
        "command_shell": shlex.join(command),
        "started_unix_ns": started_ns,
        "finished_unix_ns": finished_ns,
        "duration_seconds": round((finished_ns - started_ns) / 1e9, 6),
        "exit_code": exit_code,
        "execution_error": execution_error,
        "log": artifact(log_path, evidence_root),
        "json": None,
        "report": None,
    }
    if json_path.is_file() and json_path.stat().st_size > 0:
        result["json"] = artifact(json_path, evidence_root)
        try:
            result["report"] = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            result["execution_error"] = f"benchmark JSON is invalid: {error}"
    return result


def verify_manifest(path: Path, expected_commit: str, require_qualified: bool) -> None:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    errors: list[str] = []
    if manifest.get("schema") != ARTIFACT_SCHEMA:
        errors.append("manifest schema mismatch")
    source = manifest.get("source")
    if not isinstance(source, dict) or source.get("expected_commit") != expected_commit.lower():
        errors.append("manifest expected commit mismatch")
    else:
        for phase in ("before", "after"):
            observed = source.get(phase)
            if not isinstance(observed, dict) or observed.get("commit") != expected_commit.lower() or observed.get("state") != "clean":
                errors.append(f"manifest source {phase} is not the clean exact commit")
    configuration = manifest.get("configuration")
    if not isinstance(configuration, dict) or canonical_sha256(configuration) != manifest.get("configuration_sha256"):
        errors.append("manifest configuration SHA-256 mismatch")
    runs = manifest.get("process_runs")
    if not isinstance(runs, list) or not runs:
        errors.append("manifest process runs are missing")
    else:
        expected_run_count = configuration.get("rounds", 0) * configuration.get("processes", 0) if isinstance(configuration, dict) else 0
        if len(runs) != expected_run_count:
            errors.append("manifest process run count is incomplete")
        for run in runs:
            if not isinstance(run, dict):
                errors.append("manifest process run is malformed")
                continue
            errors.extend(verify_artifact(run.get("log"), path.parent))
            errors.extend(verify_artifact(run.get("json"), path.parent))
    outcome = manifest.get("outcome")
    eligible = manifest.get("qualification_eligible")
    embedded_errors = manifest.get("errors")
    artifacts_complete = manifest.get("artifacts_complete") is True
    target_match = isinstance(manifest.get("target_evaluation"), dict) and manifest["target_evaluation"].get("matches") is True
    sla_passed = isinstance(manifest.get("sla"), dict) and manifest["sla"].get("passed") is True
    expected_outcome, expected_eligible = derive_outcome(
        embedded_errors if isinstance(embedded_errors, list) else ["errors field malformed"],
        artifacts_complete,
        target_match,
        sla_passed,
    )
    if outcome != expected_outcome or eligible is not expected_eligible:
        errors.append("manifest outcome/qualification_eligible is not fail-closed")
    if require_qualified and (outcome != "passed" or eligible is not True or embedded_errors != []):
        errors.append("manifest is not a passed qualification result")
    if errors:
        raise QualificationError("; ".join(errors))


def run_qualification(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    output = Path(args.output_dir).resolve()
    storage_dir = Path(args.storage_dir).resolve()
    benchmark = Path(args.benchmark).resolve()
    policy_path = Path(args.policy).resolve()
    schema_path = Path(args.schema).resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    errors: list[str] = []
    process_runs: list[dict[str, Any]] = []
    reports: list[dict[str, Any]] = []
    started_ns = time.time_ns()
    expected_commit = args.expected_commit.lower()
    source_before = {"commit": "", "state": "unknown"}
    source_after = {"commit": "", "state": "unknown"}
    policy: dict[str, Any] = {}
    host = collect_host()
    mount = collect_mount(storage_dir)
    storage = collect_storage(mount, storage_dir)
    target_match = False
    target_reasons: list[str] = []

    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        errors.append("expected commit must be a full lowercase 40-character SHA-1")
    if args.rounds < 2 or args.processes < 2:
        errors.append("qualification requires at least two rounds and two concurrent processes")
    if args.timeout_seconds <= 0:
        errors.append("timeout must be positive")
    errors.extend(validate_attestation(args.hardware_attestation))
    if not storage_dir.is_dir():
        errors.append("storage directory does not exist")
    if not benchmark.is_file() or not os.access(benchmark, os.X_OK):
        errors.append("benchmark binary is missing or not executable")
    if not schema_path.is_file():
        errors.append("artifact JSON Schema is missing")
    try:
        policy = load_policy(policy_path)
    except QualificationError as error:
        errors.append(str(error))
    if policy and args.build_config != policy.get("required_build_config"):
        errors.append("build config does not exactly match the release policy")
    source_before, source_errors = git_source(repo, expected_commit)
    errors.extend(source_errors)
    if policy:
        target_match, target_reasons = evaluate_target(host, mount, storage, policy["target"])

    configuration = {
        "rounds": args.rounds,
        "processes": args.processes,
        "concurrency": "processes_concurrent_within_each_round",
        "partition_counts": REQUIRED_PARTITIONS,
        "workload": policy.get("workload", {}),
        "build_config": args.build_config,
        "storage_directory": str(storage_dir),
        "timeout_seconds": args.timeout_seconds,
        "hardware_attestation": args.hardware_attestation,
    }

    if not errors:
        qualification_target = policy["target"]["hardware_label"] if target_match else ""
        for round_number in range(1, args.rounds + 1):
            with ThreadPoolExecutor(max_workers=args.processes) as executor:
                futures = [
                    executor.submit(
                        _execute_process,
                        benchmark=benchmark,
                        repo=repo,
                        evidence_root=output,
                        storage_dir=storage_dir,
                        round_number=round_number,
                        process_number=process_number,
                        workload=policy["workload"],
                        qualification_target=qualification_target,
                        timeout_seconds=args.timeout_seconds,
                    )
                    for process_number in range(1, args.processes + 1)
                ]
                for future in as_completed(futures):
                    try:
                        process_runs.append(future.result())
                    except Exception as error:  # Process boundary must still emit a failed manifest.
                        errors.append(f"benchmark process orchestration failed: {error}")
        process_runs.sort(key=lambda item: (item["round"], item["process"]))
        for run in process_runs:
            prefix = f"round={run['round']}/process={run['process']}"
            if run["exit_code"] != 0:
                errors.append(f"{prefix}: benchmark exited {run['exit_code']}")
            if run["execution_error"]:
                errors.append(f"{prefix}: {run['execution_error']}")
            report = run.pop("report")
            if report is None:
                errors.append(f"{prefix}: benchmark JSON was not produced")
                continue
            report_errors = validate_benchmark_report(report, policy, target_match)
            errors.extend(f"{prefix}: {error}" for error in report_errors)
            if not report_errors:
                reports.append(report)

    source_after, source_errors = git_source(repo, expected_commit)
    errors.extend(f"post-run: {error}" for error in source_errors)
    expected_runs = args.rounds * args.processes
    artifacts_complete = (
        len(process_runs) == expected_runs
        and all(run.get("log") is not None and run.get("json") is not None for run in process_runs)
    )
    sla = evaluate_sla(reports, policy) if policy and len(reports) == expected_runs else {
        "aggregation": policy.get("sla", {}).get("aggregation", "minimum_across_all_processes_and_rounds"),
        "checks": [],
        "observations": [],
        "passed": False,
    }
    sla_passed = sla.get("passed") is True
    if reports and len(reports) == expected_runs and not sla_passed:
        errors.append("one or more single-writer/scaling-efficiency SLA checks failed")
    outcome, eligible = derive_outcome(
        errors, artifacts_complete, target_match, sla_passed
    )
    manifest = {
        "schema": ARTIFACT_SCHEMA,
        "qualification_id": "D6-09",
        "validation": "V-24",
        "source": {
            "expected_commit": expected_commit,
            "before": source_before,
            "after": source_after,
        },
        "configuration": configuration,
        "configuration_sha256": canonical_sha256(configuration),
        "policy": {
            "schema": policy.get("schema", ""),
            "path": str(policy_path),
            "sha256": sha256(policy_path) if policy_path.is_file() else "",
        },
        "artifact_schema": {
            "path": str(schema_path),
            "sha256": sha256(schema_path) if schema_path.is_file() else "",
        },
        "benchmark": {
            "path": str(benchmark),
            "sha256": sha256(benchmark) if benchmark.is_file() else "",
        },
        "host": host,
        "filesystem": mount,
        "storage": storage,
        "target_evaluation": {
            "matches": target_match,
            "reasons": target_reasons,
        },
        "process_runs": process_runs,
        "sla": sla,
        "artifacts_complete": artifacts_complete,
        "qualification_eligible": eligible,
        "outcome": outcome,
        "errors": errors,
        "started_unix_ns": started_ns,
        "finished_unix_ns": time.time_ns(),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 1 if outcome == "failed" else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=".")
    parser.add_argument("--benchmark")
    parser.add_argument("--output-dir")
    parser.add_argument("--storage-dir")
    parser.add_argument("--expected-commit")
    parser.add_argument("--policy")
    parser.add_argument("--schema")
    parser.add_argument("--build-config", default="")
    parser.add_argument("--hardware-attestation", default="")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--processes", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--verify-manifest")
    parser.add_argument("--require-expected-commit")
    parser.add_argument("--require-qualified", action="store_true")
    args = parser.parse_args()
    if args.verify_manifest:
        if not args.require_expected_commit:
            parser.error("--verify-manifest requires --require-expected-commit")
        return args
    required = (
        "benchmark",
        "output_dir",
        "storage_dir",
        "expected_commit",
        "policy",
        "schema",
    )
    for name in required:
        if not getattr(args, name):
            parser.error(f"--{name.replace('_', '-')} is required")
    return args


def main() -> int:
    args = parse_args()
    if args.verify_manifest:
        verify_manifest(
            Path(args.verify_manifest),
            args.require_expected_commit,
            args.require_qualified,
        )
        return 0
    return run_qualification(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (QualificationError, OSError, json.JSONDecodeError) as error:
        print(f"storage_partition_qualification: {error}", file=sys.stderr)
        sys.exit(2)
