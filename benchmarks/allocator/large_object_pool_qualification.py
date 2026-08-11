#!/usr/bin/env python3
"""Fail-closed D6-08 physical large-object-pool qualification runner."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import shlex
import shutil
import subprocess
import sys
import time
from typing import Any, cast

MANIFEST_SCHEMA = "mino.allocator.large_object_pool.qualification.v1"
BENCHMARK_SCHEMA = "mino.allocator.large_object_pool.benchmark.v1"
SLA_SCHEMA = "mino.allocator.large_object_pool.qualification_sla.v1"
QUALIFICATION_ID = "D6-08"
ATTESTATION = "physical-hugepage-device"
POOL_BYTES = 64 * 1024 * 1024
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_THRESHOLD_KEYS = {
    "min_operations_per_second",
    "max_p99_ns",
    "max_internal_fragmentation_bytes",
    "max_external_fragmentation_bytes",
    "max_hugepage_fallback_allocations",
    "max_operation_failures",
    "max_registration_failures",
    "max_deregister_errors",
    "max_coalesce_errors",
    "max_quota_errors",
}


class QualificationError(RuntimeError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact(path: Path, root: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size <= 0:
        raise QualificationError(f"required artifact is missing or empty: {path}")
    try:
        relative = path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as error:
        raise QualificationError(f"artifact escapes output root: {path}") from error
    return {"path": relative, "bytes": path.stat().st_size, "sha256": sha256(path)}


def verify_artifact(record: object, root: Path) -> None:
    if not isinstance(record, dict):
        raise QualificationError("artifact record is malformed")
    relative = record.get("path")
    size = record.get("bytes")
    digest = record.get("sha256")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise QualificationError("artifact path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as error:
        raise QualificationError("artifact path escapes output root") from error
    if (
        not path.is_file()
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size <= 0
        or path.stat().st_size != size
        or not isinstance(digest, str)
        or DIGEST_RE.fullmatch(digest) is None
        or sha256(path) != digest
    ):
        raise QualificationError(f"artifact size or SHA-256 mismatch: {relative}")


def json_object(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot read {label}: {error}") from error
    if not isinstance(value, dict):
        raise QualificationError(f"{label} must be a JSON object")
    return value


def git_source(repo: Path, expected_commit: str) -> tuple[dict[str, str], list[str]]:
    expected = expected_commit.lower()
    errors: list[str] = []
    if COMMIT_RE.fullmatch(expected) is None:
        errors.append("expected commit must be a full lowercase 40-character SHA")
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True, timeout=30
        ).strip().lower()
        status = subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=repo,
            text=True,
            timeout=30,
        ).strip()
    except (OSError, subprocess.SubprocessError) as error:
        return {"expected_commit": expected, "commit": "", "state": "unknown"}, [
            f"cannot inspect source checkout: {error}"
        ]
    state = "dirty" if status else "clean"
    if commit != expected:
        errors.append(f"HEAD {commit} does not match expected commit {expected}")
    if status:
        errors.append("qualification requires a clean worktree including untracked files")
    return {"expected_commit": expected, "commit": commit, "state": state}, errors


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def hugepage_evidence(hugetlbfs_path: Path) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    values: dict[str, int] = {}
    for line in read_text(Path("/proc/meminfo")).splitlines():
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        fields = raw.strip().split()
        if fields and fields[0].isdigit():
            values[key] = int(fields[0])
    page_bytes = values.get("Hugepagesize", 0) * 1024
    free_pages = values.get("HugePages_Free", 0)
    free_bytes = page_bytes * free_pages
    mount_type = ""
    requested = str(hugetlbfs_path.resolve()) if hugetlbfs_path.exists() else str(hugetlbfs_path)
    for line in read_text(Path("/proc/mounts")).splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[1] == requested:
            mount_type = fields[2]
            break
    if platform.system() != "Linux":
        errors.append(f"physical qualification requires Linux, got {platform.system()}")
    if not hugetlbfs_path.is_dir() or mount_type != "hugetlbfs":
        errors.append("configured HugePage path is not a mounted hugetlbfs")
    if page_bytes <= 0 or free_bytes < POOL_BYTES:
        errors.append(f"HugePage pool has {free_bytes} free bytes; {POOL_BYTES} required")
    return {
        "path": str(hugetlbfs_path),
        "mount_type": mount_type,
        "page_bytes": page_bytes,
        "free_pages": free_pages,
        "free_bytes": free_bytes,
        "required_bytes": POOL_BYTES,
    }, errors


def locked_memory_evidence() -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
    infinity = resource.RLIM_INFINITY
    soft_bytes = -1 if soft == infinity else int(soft)
    hard_bytes = -1 if hard == infinity else int(hard)
    sufficient = soft == infinity or soft >= POOL_BYTES
    if not sufficient:
        errors.append(f"RLIMIT_MEMLOCK soft limit is {soft_bytes}; {POOL_BYTES} required")
    return {
        "soft_bytes": soft_bytes,
        "hard_bytes": hard_bytes,
        "required_bytes": POOL_BYTES,
        "sufficient": sufficient,
    }, errors


def device_evidence(
    device: str, port: str, numa_node: int, sysfs_root: Path = Path("/sys")
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", device):
        errors.append("device name contains invalid characters")
    if not port.isdigit() or int(port) <= 0:
        errors.append("device port must be a positive integer")
    device_path = sysfs_root / "class" / "infiniband" / device
    port_path = device_path / "ports" / port
    state = read_text(port_path / "state")
    physical_state = read_text(port_path / "phys_state")
    link_layer = read_text(port_path / "link_layer")
    rate = read_text(port_path / "rate")
    observed_numa = read_text(device_path / "device" / "numa_node")
    driver_path = device_path / "device" / "driver"
    driver = driver_path.resolve().name if driver_path.exists() else ""
    resolved = device_path.resolve() if device_path.exists() else device_path
    devices_root = (sysfs_root / "devices").resolve()
    physical_path = False
    if device_path.exists():
        try:
            resolved.relative_to(devices_root)
            physical_path = True
        except ValueError:
            physical_path = False
    if not device_path.exists() or not physical_path:
        errors.append("RDMA device is absent or is not backed by /sys/devices")
    if "ACTIVE" not in state.upper():
        errors.append("RDMA port state is not ACTIVE")
    if "LINKUP" not in physical_state.upper():
        errors.append("RDMA physical port state is not LinkUp")
    if not link_layer:
        errors.append("RDMA link-layer provenance is missing")
    try:
        observed_numa_value = int(observed_numa)
    except ValueError:
        observed_numa_value = -1
    if observed_numa_value != numa_node:
        errors.append(
            f"device NUMA node {observed_numa_value} does not match requested node {numa_node}"
        )
    node_path = sysfs_root / "devices" / "system" / "node" / f"node{numa_node}"
    if not node_path.is_dir():
        errors.append("requested NUMA node is not present in sysfs")
    return {
        "name": device,
        "path": str(resolved),
        "sysfs_backed": physical_path,
        "port": int(port) if port.isdigit() else 0,
        "state": state,
        "physical_state": physical_state,
        "link_layer": link_layer,
        "rate": rate,
        "driver": driver,
        "numa_node": observed_numa_value,
    }, errors


def provider_evidence(plugin: Path, approved_sha256: str) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    approved = approved_sha256.lower()
    actual = ""
    if not plugin.is_absolute() or not plugin.is_file():
        errors.append("provider plugin must be an absolute regular file")
    else:
        actual = sha256(plugin)
    if DIGEST_RE.fullmatch(approved) is None:
        errors.append("approved provider SHA-256 is malformed")
    if actual != approved:
        errors.append("provider plugin SHA-256 does not match the approved SHA-256")
    return {
        "path": str(plugin),
        "sha256": actual,
        "approved_sha256": approved,
        "approved": not errors,
    }, errors


def positive_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def nonnegative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_policy(policy: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if policy.get("schema") != SLA_SCHEMA or policy.get("qualification_id") != QUALIFICATION_ID:
        errors.append("SLA policy schema or qualification ID mismatch")
    modes = policy.get("required_modes")
    sizes = policy.get("required_sizes")
    lifetimes = policy.get("required_lifetimes")
    if modes != ["ordinary", "hugepage", "device-registration"]:
        errors.append("SLA required mode matrix is not canonical")
    if sizes != [65536, 262144, 1048576]:
        errors.append("SLA required size matrix is not canonical")
    if lifetimes != ["short", "batch"]:
        errors.append("SLA required lifetime matrix is not canonical")
    minimum_iterations = policy.get("minimum_iterations")
    if not isinstance(minimum_iterations, int) or isinstance(minimum_iterations, bool) or minimum_iterations < 1000:
        errors.append("SLA minimum_iterations must be at least 1000")
    thresholds = policy.get("thresholds")
    if not isinstance(thresholds, dict) or set(thresholds) != REQUIRED_THRESHOLD_KEYS:
        errors.append("SLA threshold set is incomplete or unexpected")
        return errors
    if not positive_number(thresholds.get("min_operations_per_second")):
        errors.append("minimum throughput threshold must be positive")
    if not positive_number(thresholds.get("max_p99_ns")):
        errors.append("maximum p99 threshold must be positive")
    for key in REQUIRED_THRESHOLD_KEYS - {"min_operations_per_second", "max_p99_ns"}:
        if not nonnegative_integer(thresholds.get(key)):
            errors.append(f"SLA threshold {key} must be a non-negative integer")
    return errors


def validate_report(
    report: Mapping[str, Any],
    policy: Mapping[str, Any],
    *,
    expected_device: str,
    expected_numa_node: int,
    expected_iterations: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    errors = validate_policy(policy)
    checks: list[dict[str, Any]] = []
    if report.get("schema") != BENCHMARK_SCHEMA:
        errors.append("benchmark schema mismatch")
    if report.get("status") != "PASSED":
        errors.append(f"benchmark status is {report.get('status')!r}, not PASSED")
    if report.get("qualification_eligible") is not True:
        errors.append("benchmark is not qualification eligible")
    if report.get("attestation") != ATTESTATION:
        errors.append("benchmark physical attestation mismatch")
    config = json_object(report.get("config"))
    if config.get("iterations") != expected_iterations:
        errors.append("benchmark iteration count differs from the requested run")
    provider = json_object(report.get("provider"))
    if provider.get("class") != "device" or provider.get("device") != expected_device:
        errors.append("benchmark did not use the requested real device provider")
    if not isinstance(provider.get("name"), str) or not provider.get("name"):
        errors.append("device provider name is missing")
    if not isinstance(provider.get("provenance"), str) or not provider.get("provenance"):
        errors.append("device provider provenance is missing")
    hugepages = json_object(report.get("hugepages"))
    if hugepages.get("requested") is not True or hugepages.get("actual") is not True:
        errors.append("benchmark HugePage backing is not actual")
    if hugepages.get("fallback_reason") != "none" or hugepages.get("fallback_errno") != 0:
        errors.append("benchmark HugePage mapping reports fallback")
    locked = json_object(report.get("locked_memory"))
    if locked.get("succeeded") is not True or not isinstance(locked.get("bytes"), int) or locked.get("bytes", 0) < POOL_BYTES:
        errors.append("device registration pool was not actually memory-locked")
    numa = json_object(report.get("numa"))
    if (
        numa.get("linux_native") is not True
        or numa.get("configured_node") != expected_numa_node
        or expected_numa_node not in numa.get("allowed_nodes", [])
    ):
        errors.append("benchmark NUMA provenance does not bind the requested node")
    contract = json_object(report.get("contract"))
    for key in ("deregister_errors", "coalesce_errors", "quota_errors"):
        if contract.get(key) != 0:
            errors.append(f"benchmark contract {key} is non-zero")

    rows = report.get("rows")
    if not isinstance(rows, list):
        errors.append("benchmark rows are missing")
        return checks, errors
    modes = policy.get("required_modes", [])
    sizes = policy.get("required_sizes", [])
    lifetimes = policy.get("required_lifetimes", [])
    expected_matrix = [(mode, size, lifetime) for mode in modes for size in sizes for lifetime in lifetimes]
    observed_matrix = [
        (row.get("mode"), row.get("bytes"), row.get("lifetime"))
        for row in rows
        if isinstance(row, dict)
    ]
    if observed_matrix != expected_matrix or len(rows) != len(expected_matrix):
        errors.append("ordinary/HugePage/device size and lifetime matrix is incomplete, duplicated, or out of order")
    thresholds = policy.get("thresholds", {})
    mappings = (
        ("operations_per_second", "min_operations_per_second", "min"),
        ("p99_ns", "max_p99_ns", "max"),
        ("internal_fragmentation_bytes", "max_internal_fragmentation_bytes", "max"),
        ("external_fragmentation_bytes", "max_external_fragmentation_bytes", "max"),
        ("hugepage_fallback_allocations", "max_hugepage_fallback_allocations", "max"),
        ("failures", "max_operation_failures", "max"),
        ("registration_failures", "max_registration_failures", "max"),
        ("deregister_errors", "max_deregister_errors", "max"),
        ("coalesce_errors", "max_coalesce_errors", "max"),
        ("quota_errors", "max_quota_errors", "max"),
    )
    for index, original in enumerate(rows):
        if not isinstance(original, dict):
            errors.append(f"row {index} is malformed")
            continue
        row = original
        label = f"{row.get('mode')}[{row.get('bytes')}/{row.get('lifetime')}]"
        expected_registration_lifetime = (
            "lease"
            if row.get("mode") == "device-registration" and row.get("lifetime") == "batch"
            else "allocation"
        )
        if row.get("registration_lifetime") != expected_registration_lifetime:
            errors.append(f"{label}: registration lifetime is mislabeled")
        if row.get("operations") != expected_iterations:
            errors.append(f"{label}: operations differ from requested iterations")
        if not positive_number(row.get("operations_per_second")) or not positive_number(row.get("p99_ns")):
            errors.append(f"{label}: throughput and p99 must be positive numbers")
        for metric, threshold_name, direction in mappings:
            observed = row.get(metric)
            threshold = thresholds.get(threshold_name) if isinstance(thresholds, dict) else None
            numeric = isinstance(observed, (int, float)) and not isinstance(observed, bool)
            threshold_numeric = isinstance(threshold, (int, float)) and not isinstance(threshold, bool)
            if numeric and threshold_numeric:
                observed_number = float(cast(int | float, observed))
                threshold_number = float(cast(int | float, threshold))
                passed = (
                    observed_number >= threshold_number
                    if direction == "min"
                    else observed_number <= threshold_number
                )
            else:
                passed = False
            checks.append(
                {
                    "mode": row.get("mode"),
                    "bytes": row.get("bytes"),
                    "lifetime": row.get("lifetime"),
                    "metric": metric,
                    "observed": observed,
                    "threshold": threshold,
                    "passed": passed,
                }
            )
            if not passed:
                errors.append(f"{label}: SLA {metric}={observed!r} failed {threshold_name}={threshold!r}")
        numa_metrics = json_object(row.get("numa"))
        if numa_metrics.get("fallback_allocations") != 0 or numa_metrics.get("bind_errors") != 0:
            errors.append(f"{label}: NUMA fallback or bind errors are non-zero")
    return checks, errors


def verify_manifest(path: Path, expected_commit: str) -> None:
    manifest = load_json(path, "qualification manifest")
    errors: list[str] = []
    if manifest.get("schema") != MANIFEST_SCHEMA:
        errors.append("manifest schema mismatch")
    source = json_object(manifest.get("source"))
    if source != {
        "expected_commit": expected_commit.lower(),
        "commit": expected_commit.lower(),
        "state": "clean",
    }:
        errors.append("manifest is not bound to the clean exact expected commit")
    for key, expected in (
        ("attestation", ATTESTATION),
        ("qualification_id", QUALIFICATION_ID),
        ("artifacts_complete", True),
        ("outcome", "passed"),
        ("qualification_eligible", True),
    ):
        if manifest.get(key) != expected:
            errors.append(f"manifest {key} differs from {expected!r}")
    if manifest.get("errors") != []:
        errors.append("manifest contains qualification errors")
    artifacts = manifest.get("artifacts")
    expected_artifact_paths = {
        "preflight.json",
        "sla_policy.json",
        "artifact.schema.json",
        "benchmark.json",
        "benchmark.log",
    }
    records_by_path: dict[str, dict[str, Any]] = {}
    if not isinstance(artifacts, list) or len(artifacts) != 5:
        errors.append("manifest must contain exactly five hashed artifacts")
    else:
        for record in artifacts:
            try:
                verify_artifact(record, path.parent)
                if isinstance(record, dict) and isinstance(record.get("path"), str):
                    records_by_path[record["path"]] = record
            except QualificationError as error:
                errors.append(str(error))
        if set(records_by_path) != expected_artifact_paths:
            errors.append("manifest artifact path set is not the canonical five-file evidence set")
    inputs = json_object(manifest.get("inputs"))
    sla = json_object(manifest.get("sla"))
    preflight = json_object(manifest.get("preflight"))
    preflight_provider = json_object(preflight.get("provider"))
    policy_record = records_by_path.get("sla_policy.json", {})
    schema_record = records_by_path.get("artifact.schema.json", {})
    if inputs.get("policy_sha256") != policy_record.get("sha256") or sla.get(
        "policy_sha256"
    ) != policy_record.get("sha256"):
        errors.append("manifest SLA policy hashes are not bound to the archived policy")
    if inputs.get("schema_sha256") != schema_record.get("sha256"):
        errors.append("manifest schema hash is not bound to the archived schema")
    if (
        inputs.get("plugin_sha256") != preflight_provider.get("sha256")
        or inputs.get("approved_plugin_sha256")
        != preflight_provider.get("approved_sha256")
        or inputs.get("plugin_sha256") != inputs.get("approved_plugin_sha256")
    ):
        errors.append("manifest plugin hashes are not bound to approved preflight evidence")
    if errors:
        raise QualificationError("; ".join(errors))


def run_qualification(args: argparse.Namespace) -> int:
    started = time.time()
    repo = Path(args.repo).resolve()
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "benchmark.json"
    log_path = output / "benchmark.log"
    preflight_path = output / "preflight.json"
    policy_copy = output / "sla_policy.json"
    schema_copy = output / "artifact.schema.json"
    manifest_path = output / "manifest.json"
    errors: list[str] = []

    benchmark = Path(args.benchmark).resolve()
    plugin = Path(args.plugin)
    policy_path = Path(args.sla_policy).resolve()
    schema_path = Path(args.artifact_schema).resolve()
    source, source_errors = git_source(repo, args.expected_commit)
    errors.extend(source_errors)

    required_files = ((benchmark, "benchmark"), (policy_path, "SLA policy"), (schema_path, "artifact schema"))
    for path, label in required_files:
        if not path.is_file():
            errors.append(f"{label} is missing: {path}")
    if benchmark.is_file() and not os.access(benchmark, os.X_OK):
        errors.append("benchmark is not executable")
    if policy_path.is_file():
        shutil.copyfile(policy_path, policy_copy)
    if schema_path.is_file():
        shutil.copyfile(schema_path, schema_copy)

    try:
        numa_node = int(args.numa_node)
        if numa_node < 0:
            raise ValueError
    except ValueError:
        numa_node = -1
        errors.append("NUMA node must be a non-negative integer")
    if args.iterations < 1000:
        errors.append("qualification requires at least 1000 iterations")
    if args.physical_attestation != ATTESTATION:
        errors.append(f"physical attestation must be exactly {ATTESTATION}")

    hugepages, huge_errors = hugepage_evidence(Path(args.hugetlbfs_path))
    locked, lock_errors = locked_memory_evidence()
    device, device_errors = device_evidence(args.device, args.port, numa_node)
    provider, provider_errors = provider_evidence(plugin, args.approved_plugin_sha256)
    errors.extend(huge_errors)
    errors.extend(lock_errors)
    errors.extend(device_errors)
    errors.extend(provider_errors)

    policy: dict[str, Any] = {}
    if policy_path.is_file():
        try:
            policy = load_json(policy_path, "SLA policy")
            errors.extend(validate_policy(policy))
        except QualificationError as error:
            errors.append(str(error))
    schema_document: dict[str, Any] = {}
    if schema_path.is_file():
        try:
            schema_document = load_json(schema_path, "artifact schema")
            if schema_document.get("properties", {}).get("schema", {}).get("const") != MANIFEST_SCHEMA:
                errors.append("artifact schema is not bound to the manifest version")
        except QualificationError as error:
            errors.append(str(error))

    preflight = {
        "passed": not errors,
        "source": source,
        "hugepages": hugepages,
        "locked_memory": locked,
        "device": device,
        "numa": {"requested_node": numa_node, "node_present": not any("NUMA node" in error for error in device_errors)},
        "provider": provider,
        "errors": list(errors),
    }
    preflight_path.write_text(json.dumps(preflight, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    checks: list[dict[str, Any]] = []
    if not errors:
        command = [
            str(benchmark),
            f"--iterations={args.iterations}",
            f"--json={report_path}",
            f"--plugin={plugin}",
            f"--device={args.device}",
            f"--hugetlbfs-path={args.hugetlbfs_path}",
            f"--numa-node={numa_node}",
            f"--qualification-attestation={ATTESTATION}",
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=repo,
                check=False,
                capture_output=True,
                text=True,
                timeout=args.timeout_seconds,
            )
            log_path.write_text(
                "$ " + shlex.join(command) + "\n\nSTDOUT\n" + completed.stdout + "\nSTDERR\n" + completed.stderr,
                encoding="utf-8",
            )
            if completed.returncode != 0:
                errors.append(f"benchmark exited {completed.returncode}")
            if not report_path.is_file():
                errors.append("benchmark JSON was not produced")
            else:
                report = load_json(report_path, "benchmark report")
                report_checks, report_errors = validate_report(
                    report,
                    policy,
                    expected_device=args.device,
                    expected_numa_node=numa_node,
                    expected_iterations=args.iterations,
                )
                checks.extend(report_checks)
                errors.extend(report_errors)
        except (OSError, subprocess.SubprocessError, QualificationError) as error:
            errors.append(str(error))

    available = [preflight_path, policy_copy, schema_copy, report_path, log_path]
    records: list[dict[str, Any]] = []
    for path in available:
        if path.is_file() and path.stat().st_size > 0:
            try:
                records.append(artifact(path, output))
            except QualificationError as error:
                errors.append(str(error))
    artifacts_complete = len(records) == 5
    if not artifacts_complete:
        errors.append("qualification artifact set is incomplete")
    sla_passed = bool(checks) and all(check.get("passed") is True for check in checks)
    if not sla_passed and report_path.is_file():
        errors.append("one or more SLA checks failed or are missing")
    passed = not errors and artifacts_complete and sla_passed

    benchmark_digest = sha256(benchmark) if benchmark.is_file() else ""
    plugin_digest = sha256(plugin) if plugin.is_file() else ""
    policy_digest = sha256(policy_path) if policy_path.is_file() else ""
    schema_digest = sha256(schema_path) if schema_path.is_file() else ""
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "qualification_id": QUALIFICATION_ID,
        "source": source,
        "attestation": args.physical_attestation,
        "host": {
            "system": platform.system(),
            "kernel": platform.release(),
            "machine": platform.machine(),
            "hostname": platform.node(),
        },
        "inputs": {
            "benchmark_sha256": benchmark_digest,
            "plugin_sha256": plugin_digest,
            "approved_plugin_sha256": args.approved_plugin_sha256.lower(),
            "policy_sha256": policy_digest,
            "schema_sha256": schema_digest,
        },
        "preflight": preflight,
        "sla": {"policy_sha256": policy_digest, "checks": checks, "passed": sla_passed},
        "artifacts": records,
        "artifacts_complete": artifacts_complete,
        "outcome": "passed" if passed else "failed",
        "qualification_eligible": passed,
        "errors": errors,
        "duration_seconds": round(time.time() - started, 3),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if passed else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=".")
    parser.add_argument("--benchmark")
    parser.add_argument("--plugin")
    parser.add_argument("--approved-plugin-sha256")
    parser.add_argument("--device")
    parser.add_argument("--port", default="1")
    parser.add_argument("--hugetlbfs-path")
    parser.add_argument("--numa-node")
    parser.add_argument("--iterations", type=int, default=10000)
    parser.add_argument("--expected-commit")
    parser.add_argument("--physical-attestation", default="")
    parser.add_argument("--sla-policy")
    parser.add_argument("--artifact-schema")
    parser.add_argument("--output-dir")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--verify-manifest")
    parser.add_argument("--require-expected-commit")
    args = parser.parse_args()
    if args.verify_manifest:
        if not args.require_expected_commit:
            parser.error("--verify-manifest requires --require-expected-commit")
        return args
    required = (
        "benchmark",
        "plugin",
        "approved_plugin_sha256",
        "device",
        "hugetlbfs_path",
        "numa_node",
        "expected_commit",
        "sla_policy",
        "artifact_schema",
        "output_dir",
    )
    for name in required:
        if getattr(args, name) in (None, ""):
            parser.error(f"--{name.replace('_', '-')} is required")
    return args


def main() -> int:
    args = parse_args()
    if args.verify_manifest:
        verify_manifest(Path(args.verify_manifest), args.require_expected_commit)
        return 0
    return run_qualification(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (QualificationError, OSError, json.JSONDecodeError) as error:
        print(f"large_object_pool_qualification: {error}", file=sys.stderr)
        sys.exit(2)
