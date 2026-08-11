#!/usr/bin/env python3
"""Shared fail-closed contract and finalizer for physical transport qualification."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import ipaddress
import json
from pathlib import Path
import platform
import re
import shlex
import socket
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence, TypeGuard, cast

SCHEMA = "mino.transport.qualification.v2"
SLA_SCHEMA = "mino.transport.qualification.sla.v1"
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
REQUIRED_PAYLOADS = (128, 1024, 65536, 1048576)
MIN_ITERATIONS = 1000
REQUIRED_METRICS = (
    "p50_rtt_ns",
    "p99_rtt_ns",
    "process_cpu_ns",
    "payload_bytes_per_second",
    "elapsed_ns",
)


class QualificationError(RuntimeError):
    """The evidence cannot qualify."""


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
        raise QualificationError(f"artifact escapes its role root: {path}") from error
    return {"path": relative, "bytes": path.stat().st_size, "sha256": sha256(path)}


def verify_artifact(record: Any, root: Path, label: str) -> Path:
    if not isinstance(record, dict):
        raise QualificationError(f"{label} artifact record is missing")
    relative = record.get("path")
    size = record.get("bytes")
    digest = record.get("sha256")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise QualificationError(f"{label} artifact path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as error:
        raise QualificationError(f"{label} artifact path escapes its root") from error
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
        raise QualificationError(f"{label} artifact size or SHA-256 mismatch")
    return path


def json_object(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def json_array(value: object) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], value) if isinstance(value, list) else []


def load_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot read {label}: {error}") from error
    if not isinstance(value, dict):
        raise QualificationError(f"{label} must be a JSON object")
    return value


def load_policy(path: Path, qualification: str) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = load_json_object(path, "SLA policy")
    if policy.get("schema") != SLA_SCHEMA:
        raise QualificationError("SLA policy schema mismatch")
    qualifications = policy.get("qualifications")
    selected = qualifications.get(qualification) if isinstance(qualifications, dict) else None
    if not isinstance(selected, dict) or not isinstance(selected.get("cases"), dict):
        raise QualificationError(f"SLA policy has no {qualification!r} case policy")
    return policy, selected


def git_source(repo: Path, expected_commit: str) -> tuple[dict[str, str], list[str]]:
    errors: list[str] = []
    expected = expected_commit.lower()
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


def validate_run_binding(run_id: str, run_attempt: str, session_nonce: str) -> list[str]:
    errors: list[str] = []
    if not run_id.isdigit() or int(run_id) <= 0:
        errors.append("run ID must be a positive decimal identifier")
    if not run_attempt.isdigit() or int(run_attempt) <= 0:
        errors.append("run attempt must be a positive decimal identifier")
    if len(session_nonce) < 16 or len(session_nonce) > 256 or any(c.isspace() for c in session_nonce):
        errors.append("session nonce must be 16..256 non-whitespace characters")
    return errors


def validate_endpoint(address: str, port: int) -> list[str]:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return [f"endpoint address must be a literal IP address: {address!r}"]
    errors: list[str] = []
    if parsed.is_loopback or parsed.is_unspecified or parsed.is_multicast:
        errors.append(f"endpoint address is not a physical peer address: {address}")
    if not isinstance(port, int) or isinstance(port, bool) or not 1024 <= port <= 65535:
        errors.append(f"endpoint port is invalid: {port!r}")
    return errors


def endpoint_identity(address: str, port: int, node_id: int, security_domain: int) -> dict[str, Any]:
    return {
        "endpoint": f"[{address}]:{port}" if ":" in address else f"{address}:{port}",
        "node_id": node_id,
        "security_domain": security_domain,
    }


def identity_errors(local: Mapping[str, Any], peer: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for label, value in (("local", local), ("peer", peer)):
        if not isinstance(value.get("endpoint"), str) or not value["endpoint"]:
            errors.append(f"{label} endpoint is missing")
        for key in ("node_id", "security_domain"):
            candidate = value.get(key)
            if not isinstance(candidate, int) or isinstance(candidate, bool) or candidate <= 0:
                errors.append(f"{label} {key} must be a positive integer")
    if local == peer:
        errors.append("local and peer identities must differ")
    if local.get("node_id") == peer.get("node_id"):
        errors.append("local and peer node IDs must differ")
    if local.get("security_domain") == peer.get("security_domain"):
        errors.append("qualification must cross security domains")
    if local.get("endpoint") == peer.get("endpoint"):
        errors.append("local and peer endpoints must differ")
    return errors


def plugin_provenance(plugin: Path, qualification: str) -> tuple[int, str]:
    prefix = "rdma" if qualification == "rdma" else "fabric"
    try:
        library = ctypes.CDLL(str(plugin))
        abi_function = getattr(library, f"mino_{prefix}_provider_abi_version_v1")
        abi_function.argtypes = []
        abi_function.restype = ctypes.c_uint32
        provenance_function = getattr(library, f"mino_{prefix}_provider_provenance_v1")
        provenance_function.argtypes = []
        provenance_function.restype = ctypes.c_char_p
        abi = int(abi_function())
        raw = provenance_function()
    except (OSError, AttributeError) as error:
        raise QualificationError(f"provider plugin ABI/provenance exports are invalid: {error}") from error
    if abi != 1 or raw is None:
        raise QualificationError("provider plugin ABI must be v1 and provenance must be present")
    try:
        provenance = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise QualificationError("provider provenance is not UTF-8") from error
    if not provenance.strip() or len(provenance) > 4096:
        raise QualificationError("provider provenance is empty or unreasonably large")
    return abi, provenance


def provider_evidence(
    plugin: Path, qualification: str, approved_sha256: str
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    actual = sha256(plugin)
    approved = approved_sha256.lower()
    if DIGEST_RE.fullmatch(approved) is None:
        errors.append("approved provider SHA-256 is malformed")
    if actual != approved:
        errors.append("provider plugin SHA-256 does not match the approved SHA-256")
    abi = 0
    provenance = ""
    if not errors:
        try:
            abi, provenance = plugin_provenance(plugin, qualification)
        except QualificationError as error:
            errors.append(str(error))
    verified = not errors
    return {
        "plugin": str(plugin),
        "plugin_sha256": actual,
        "approved_plugin_sha256": approved,
        "abi_version": abi,
        "class": "device" if verified else "unverified",
        "class_validation": "production-loader-enforced" if verified else "not-run",
        "provenance": provenance,
    }, errors


def positive_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def expected_case_ids(qualification: str, kinds: Sequence[str]) -> list[str]:
    if qualification == "rdma":
        return ["tcp", "udp", "rdma", "rdma-zero-copy"]
    return ["tcp", *[f"fabric-{kind}" for kind in kinds]]


def expected_transport_and_copy(case_id: str) -> tuple[str, str]:
    if case_id == "rdma-zero-copy":
        return "rdma", "registered-zero-copy"
    if case_id.startswith("fabric-"):
        return "fabric", "driver-staging"
    return case_id, "driver-staging"


def parse_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise QualificationError(f"{path.name}:{number}: invalid JSON: {error}") from error
        if not isinstance(row, dict):
            raise QualificationError(f"{path.name}:{number}: row is not an object")
        rows.append(row)
    if not rows:
        raise QualificationError(f"{path.name}: result matrix is empty")
    return rows


def validate_rows(
    *,
    case_id: str,
    rows: Sequence[Mapping[str, Any]],
    payloads: Sequence[int],
    iterations: int,
    provider_provenance_value: str,
    case_policy: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    checks: list[dict[str, Any]] = []
    expected_transport, expected_copy = expected_transport_and_copy(case_id)
    expected_payloads = list(payloads)
    observed_payloads = [row.get("payload_bytes") for row in rows]
    if observed_payloads != expected_payloads or len(rows) != len(expected_payloads):
        errors.append(f"{case_id}: payload matrix is incomplete, duplicated, or out of order")
    expected_policy_keys = {
        "max_p50_rtt_ns",
        "max_p99_rtt_ns",
        "max_process_cpu_ns",
        "min_payload_bytes_per_second",
        "max_elapsed_ns",
        "copy_mode",
    }
    if set(case_policy) != expected_policy_keys:
        errors.append(f"{case_id}: SLA policy keys are incomplete or unexpected")
    for key in expected_policy_keys - {"copy_mode"}:
        if not positive_integer(case_policy.get(key)):
            errors.append(f"{case_id}: SLA threshold {key} must be a positive integer")
    if case_policy.get("copy_mode") != expected_copy:
        errors.append(f"{case_id}: SLA copy_mode policy does not match the promised path")

    normalized: list[dict[str, Any]] = []
    for index, original in enumerate(rows):
        row = dict(original)
        label = f"{case_id}[{index}]"
        if row.get("transport") != expected_transport:
            errors.append(f"{label}: transport is not {expected_transport!r}")
        if row.get("copy_mode") != expected_copy:
            errors.append(f"{label}: copy_mode is not {expected_copy!r}")
        if row.get("iterations") != iterations:
            errors.append(f"{label}: iterations differs from the requested matrix")
        for metric in REQUIRED_METRICS:
            if not positive_integer(row.get(metric)):
                errors.append(f"{label}: {metric} must be a positive integer")
        p50 = row.get("p50_rtt_ns")
        p99 = row.get("p99_rtt_ns")
        if positive_integer(p50) and positive_integer(p99) and p99 < p50:
            errors.append(f"{label}: p99_rtt_ns is lower than p50_rtt_ns")
        provenance = row.get("provider_provenance")
        if not isinstance(provenance, str) or not provenance:
            errors.append(f"{label}: provider provenance is missing")
        elif case_id not in ("tcp", "udp") and provenance != provider_provenance_value:
            errors.append(f"{label}: provider provenance differs from the approved plugin")

        policies = (
            ("p50_rtt_ns", "max_p50_rtt_ns", "max"),
            ("p99_rtt_ns", "max_p99_rtt_ns", "max"),
            ("process_cpu_ns", "max_process_cpu_ns", "max"),
            ("payload_bytes_per_second", "min_payload_bytes_per_second", "min"),
            ("elapsed_ns", "max_elapsed_ns", "max"),
        )
        for metric, threshold_name, direction in policies:
            observed = row.get(metric)
            threshold = case_policy.get(threshold_name)
            passed = (
                positive_integer(observed)
                and positive_integer(threshold)
                and (observed <= threshold if direction == "max" else observed >= threshold)
            )
            check = {
                "case": case_id,
                "payload_bytes": row.get("payload_bytes"),
                "metric": metric,
                "observed": observed,
                "policy": threshold_name,
                "threshold": threshold,
                "passed": passed,
            }
            checks.append(check)
            if not passed:
                errors.append(
                    f"{label}: SLA {metric}={observed!r} failed {threshold_name}={threshold!r}"
                )
        copy_passed = row.get("copy_mode") == case_policy.get("copy_mode") == expected_copy
        checks.append(
            {
                "case": case_id,
                "payload_bytes": row.get("payload_bytes"),
                "metric": "copy_mode",
                "observed": row.get("copy_mode"),
                "policy": "copy_mode",
                "threshold": case_policy.get("copy_mode"),
                "passed": copy_passed,
            }
        )
        if not copy_passed:
            errors.append(f"{label}: SLA copy_mode failed")
        normalized.append(row)
    return normalized, checks, errors


def run_case(
    *, command: list[str], log_path: Path, timeout_seconds: int
) -> tuple[int, int]:
    started_ns = time.time_ns()
    with log_path.open("w", encoding="utf-8") as log:
        log.write("$ " + shlex.join(command) + "\n")
        log.flush()
        completed = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    return completed.returncode, time.time_ns() - started_ns


def role_manifest_base(
    *,
    qualification: str,
    qualification_id: str,
    role: str,
    run_id: str,
    run_attempt: str,
    session_nonce: str,
    source: Mapping[str, Any],
    local: Mapping[str, Any],
    peer: Mapping[str, Any],
    payloads: Sequence[int],
    iterations: int,
    kinds: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "artifact_type": "role",
        "qualification": qualification,
        "qualification_id": qualification_id,
        "role": role,
        "run": {
            "id": run_id,
            "attempt": run_attempt,
            "session_nonce": session_nonce,
        },
        "source": dict(source),
        "identity": {
            "local": dict(local),
            "peer": dict(peer),
            "hostname": socket.gethostname(),
        },
        "host": {
            "system": platform.system(),
            "kernel": platform.release(),
            "machine": platform.machine(),
        },
        "matrix": {
            "payloads": list(payloads),
            "iterations": iterations,
            "kinds": list(kinds),
            "expected_cases": expected_case_ids(qualification, kinds),
            "cases": [],
        },
        "sla": {"policy": {}, "policy_sha256": "", "checks": [], "passed": False},
        "artifacts": [],
        "errors": [],
        "artifacts_complete": False,
        "qualification_eligible": False,
        "outcome": "failed",
    }


def write_role_manifest(output: Path, manifest: dict[str, Any], errors: Sequence[str]) -> int:
    manifest["errors"] = list(errors)
    passed = not errors and manifest.get("artifacts_complete") is True and manifest.get("sla", {}).get("passed") is True
    manifest["outcome"] = "passed" if passed else "failed"
    # A unilateral artifact is never sufficient qualification evidence.
    manifest["qualification_eligible"] = False
    manifest["finished_unix_ns"] = time.time_ns()
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0 if passed else 1


def validate_role_manifest(
    manifest: Mapping[str, Any],
    root: Path,
    role: str,
    qualification: str,
    expected_commit: str,
    expected_run_id: str,
    expected_run_attempt: str,
    expected_kinds: Sequence[str],
    policy: Mapping[str, Any],
    policy_sha256: str,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    for key, expected in (
        ("schema", SCHEMA),
        ("artifact_type", "role"),
        ("qualification", qualification),
        ("qualification_id", "D6-06" if qualification == "rdma" else "D6-07"),
        ("role", role),
        ("outcome", "passed"),
        ("artifacts_complete", True),
        ("qualification_eligible", False),
    ):
        if manifest.get(key) != expected:
            errors.append(f"{role}: {key} differs from {expected!r}")
    if manifest.get("errors") != []:
        errors.append(f"{role}: role manifest contains errors")
    run = json_object(manifest.get("run"))
    if run.get("id") != expected_run_id or run.get("attempt") != expected_run_attempt:
        errors.append(f"{role}: run ID or attempt mismatch")
    errors.extend(f"{role}: {error}" for error in validate_run_binding(str(run.get("id", "")), str(run.get("attempt", "")), str(run.get("session_nonce", ""))))
    source = json_object(manifest.get("source"))
    if source != {"expected_commit": expected_commit, "commit": expected_commit, "state": "clean"}:
        errors.append(f"{role}: source is not the clean exact expected commit")
    benchmark = json_object(manifest.get("benchmark"))
    if not isinstance(benchmark.get("name"), str) or not benchmark.get("name") or not isinstance(benchmark.get("sha256"), str) or DIGEST_RE.fullmatch(benchmark["sha256"]) is None:
        errors.append(f"{role}: benchmark identity or SHA-256 is invalid")
    provider = json_object(manifest.get("provider"))
    plugin_path = provider.get("plugin")
    if not isinstance(plugin_path, str) or not Path(plugin_path).is_absolute():
        errors.append(f"{role}: provider plugin path is not absolute")
    plugin_sha = provider.get("plugin_sha256")
    approved_sha = provider.get("approved_plugin_sha256")
    if not isinstance(plugin_sha, str) or DIGEST_RE.fullmatch(plugin_sha) is None or plugin_sha != approved_sha:
        errors.append(f"{role}: provider plugin does not match its approved SHA-256")
    if provider.get("abi_version") != 1 or provider.get("class") != "device" or provider.get("class_validation") != "production-loader-enforced":
        errors.append(f"{role}: real device provider ABI/class was not enforced")
    provenance_value = provider.get("provenance")
    provenance = provenance_value if isinstance(provenance_value, str) else ""
    if not provenance:
        errors.append(f"{role}: provider provenance is absent")
    physical = json_object(manifest.get("physical"))
    if qualification == "rdma":
        ports = physical.get("ports")
        links_valid = isinstance(ports, list) and bool(ports) and all(
            isinstance(port, dict)
            and "ACTIVE" in str(port.get("state", "")).upper()
            and "LINKUP" in str(port.get("physical_state", "")).upper()
            and bool(port.get("link_layer"))
            and bool(port.get("rate"))
            for port in ports
        )
        if physical.get("sysfs_class") != "infiniband" or not physical.get("driver") or not links_valid:
            errors.append(f"{role}: real RDMA device/class/link evidence is incomplete")
    else:
        state = str(physical.get("link_state", "")).upper()
        link_valid = any(token in state for token in ("ACTIVE", "LINKUP", "ONLINE", "UP")) and not any(
            token in state for token in ("DOWN", "INACTIVE", "OFFLINE", "DISABLED", "ERROR")
        )
        if physical.get("validated_kinds") != list(expected_kinds) or not physical.get("driver") or (not physical.get("sysfs_subsystem") and not physical.get("sysfs_class")) or not link_valid:
            errors.append(f"{role}: real Fabric device/class/link/kind evidence is incomplete")
    sla = json_object(manifest.get("sla"))
    if sla.get("passed") is not True or sla.get("policy") != policy or sla.get("policy_sha256") != policy_sha256:
        errors.append(f"{role}: SLA policy binding or result mismatch")
    matrix = json_object(manifest.get("matrix"))
    payloads = matrix.get("payloads")
    iterations = matrix.get("iterations")
    cases = matrix.get("cases")
    expected_cases = expected_case_ids(qualification, expected_kinds)
    if matrix.get("kinds") != list(expected_kinds) or matrix.get("expected_cases") != expected_cases:
        errors.append(f"{role}: promised kind/case matrix mismatch")
    if payloads != list(REQUIRED_PAYLOADS):
        errors.append(f"{role}: payload matrix must be exactly {list(REQUIRED_PAYLOADS)}")
        validated_payloads: Sequence[int] = []
    else:
        validated_payloads = REQUIRED_PAYLOADS
    if not positive_integer(iterations) or iterations < MIN_ITERATIONS:
        errors.append(f"{role}: iteration count must be at least {MIN_ITERATIONS}")
        iterations = 0
    if not isinstance(cases, list) or [case.get("case") for case in cases if isinstance(case, dict)] != expected_cases or len(cases) != len(expected_cases):
        errors.append(f"{role}: case matrix is incomplete, duplicated, or out of order")
        cases = []
    recomputed_checks: list[dict[str, Any]] = []
    expected_artifact_records: list[dict[str, Any]] = []
    for case in cases:
        case_id = case.get("case")
        try:
            log_path = verify_artifact(case.get("log"), root, f"{role}/{case_id} log")
            expected_artifact_records.append(case["log"])
            if log_path.stat().st_size <= 0:
                errors.append(f"{role}/{case_id}: log is empty")
            if role == "client":
                result_path = verify_artifact(case.get("result"), root, f"{role}/{case_id} result")
                expected_artifact_records.append(case["result"])
                rows = parse_jsonl(result_path)
                normalized, checks, row_errors = validate_rows(
                    case_id=case_id,
                    rows=rows,
                    payloads=validated_payloads,
                    iterations=iterations,
                    provider_provenance_value=provenance,
                    case_policy=policy.get("cases", {}).get(case_id, {}),
                )
                recomputed_checks.extend(checks)
                errors.extend(f"{role}: {error}" for error in row_errors)
                if case.get("rows") != normalized:
                    errors.append(f"{role}/{case_id}: manifest rows differ from hashed result artifact")
            elif case.get("result") is not None or case.get("rows") != []:
                errors.append(f"{role}/{case_id}: server must not claim client measurements")
        except (QualificationError, OSError) as error:
            errors.append(str(error))
    if manifest.get("artifacts") != expected_artifact_records:
        errors.append(f"{role}: top-level artifact inventory differs from the complete case inventory")
    if role == "client" and sla.get("checks") != recomputed_checks:
        errors.append("client: recorded SLA checks differ from independently recomputed checks")
    if role == "server" and sla.get("checks") != []:
        errors.append("server: SLA checks must be empty")
    return dict(manifest), errors


def finalize(
    *,
    qualification: str,
    server_root: Path,
    client_root: Path,
    output_path: Path,
    policy_path: Path,
    expected_commit: str,
    expected_run_id: str,
    expected_run_attempt: str,
    expected_kinds: Sequence[str],
) -> int:
    errors: list[str] = []
    expected_commit = expected_commit.lower()
    if qualification not in ("rdma", "fabric"):
        errors.append("qualification must be rdma or fabric")
    if COMMIT_RE.fullmatch(expected_commit) is None:
        errors.append("finalizer expected commit must be a full lowercase 40-character SHA")
    errors.extend(validate_run_binding(expected_run_id, expected_run_attempt, "finalizer-binding-nonce"))
    if qualification == "rdma" and expected_kinds:
        errors.append("RDMA finalization must not contain Fabric kinds")
    if qualification == "fabric" and list(expected_kinds) != ["ipcf", "ntb", "cxl"]:
        errors.append("Fabric finalization requires exactly IPCF, NTB, and CXL")
    final: dict[str, Any] = {
        "schema": SCHEMA,
        "artifact_type": "final",
        "qualification": qualification,
        "qualification_id": "D6-06" if qualification == "rdma" else "D6-07",
        "run": {"id": expected_run_id, "attempt": expected_run_attempt, "session_nonce": ""},
        "source": {"expected_commit": expected_commit, "commit": expected_commit, "state": "clean"},
        "expected_kinds": list(expected_kinds),
        "role_artifacts": {},
        "results": [],
        "sla": {"passed": False, "checks": []},
        "errors": [],
        "artifacts_complete": False,
        "qualification_eligible": False,
        "outcome": "failed",
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    policy: dict[str, Any] = {}
    policy_file_hash = ""
    try:
        _, policy = load_policy(policy_path, qualification)
        policy_file_hash = sha256(policy_path)
    except QualificationError as error:
        errors.append(str(error))
    roles: dict[str, dict[str, Any]] = {}
    for role, root in (("server", server_root), ("client", client_root)):
        manifest_path = root / "manifest.json"
        try:
            manifest = load_json_object(manifest_path, f"{role} role manifest")
            roles[role], role_errors = validate_role_manifest(
                manifest,
                root,
                role,
                qualification,
                expected_commit,
                expected_run_id,
                expected_run_attempt,
                expected_kinds,
                policy,
                policy_file_hash,
            )
            errors.extend(role_errors)
            final["role_artifacts"][role] = artifact(manifest_path, root)
        except (QualificationError, OSError) as error:
            errors.append(str(error))

    server = roles.get("server", {})
    client = roles.get("client", {})
    server_run = json_object(server.get("run"))
    client_run = json_object(client.get("run"))
    nonce = server_run.get("session_nonce")
    final["run"]["session_nonce"] = nonce if isinstance(nonce, str) else ""
    if server_run != client_run:
        errors.append("server and client run ID/attempt/session nonce differ")
    if server_run.get("id") != expected_run_id or server_run.get("attempt") != expected_run_attempt:
        errors.append("role run binding differs from finalizer expectation")

    server_identity = json_object(server.get("identity"))
    client_identity = json_object(client.get("identity"))
    if server_identity.get("local") != client_identity.get("peer"):
        errors.append("server local identity/endpoint/node/domain is not the client peer")
    if server_identity.get("peer") != client_identity.get("local"):
        errors.append("server peer identity/endpoint/node/domain is not the client local identity")
    if server_identity.get("hostname") == client_identity.get("hostname"):
        errors.append("server and client hostnames must differ")
    final["identities"] = {"server": server_identity, "client": client_identity}

    for field, label in (("source", "source"), ("benchmark", "benchmark")):
        if server.get(field) != client.get(field):
            errors.append(f"server and client {label} evidence differ")
        final[field] = server.get(field, final.get(field, {}))
    server_provider = json_object(server.get("provider"))
    client_provider = json_object(client.get("provider"))
    provider_binding_fields = (
        "plugin_sha256",
        "approved_plugin_sha256",
        "abi_version",
        "class",
        "class_validation",
        "provenance",
    )
    if any(server_provider.get(field) != client_provider.get(field) for field in provider_binding_fields):
        errors.append("server and client approved plugin hash/ABI/class/provenance differ")
    final["provider"] = {
        field: server_provider.get(field) for field in provider_binding_fields
    }
    final["provider"]["role_plugin_paths"] = {
        "server": server_provider.get("plugin"),
        "client": client_provider.get("plugin"),
    }
    server_matrix = json_object(server.get("matrix"))
    client_matrix = json_object(client.get("matrix"))
    if server_matrix.get("payloads") != client_matrix.get("payloads") or server_matrix.get("iterations") != client_matrix.get("iterations"):
        errors.append("server and client payload/iteration matrices differ")

    client_sla = json_object(client.get("sla"))
    all_checks = json_array(client_sla.get("checks"))
    final["sla"] = {
        "policy_sha256": policy_file_hash,
        "checks": all_checks,
        "passed": bool(all_checks) and all(check.get("passed") is True for check in all_checks),
    }
    if not final["sla"]["passed"]:
        errors.append("independent client metric SLA checks did not all pass")

    result_kinds = ["rdma"] if qualification == "rdma" else list(expected_kinds)
    for kind in result_kinds:
        prefix = ("rdma", "rdma-zero-copy") if kind == "rdma" else (f"fabric-{kind}",)
        relevant = [check for check in all_checks if check.get("case") in prefix]
        passed = bool(relevant) and all(check.get("passed") is True for check in relevant)
        final["results"].append({"kind": kind, "outcome": "passed" if passed else "failed", "sla_passed": passed})
        if not passed:
            errors.append(f"{qualification} final result for {kind} did not pass")
    if qualification == "fabric" and set(item["kind"] for item in final["results"]) != {"ipcf", "ntb", "cxl"}:
        errors.append("Fabric qualification must produce separate IPCF, NTB, and CXL final results")

    final["physical"] = {"server": server.get("physical"), "client": client.get("physical")}
    complete = not errors and set(final["role_artifacts"]) == {"server", "client"}
    final["artifacts_complete"] = complete
    final["qualification_eligible"] = complete and final["sla"]["passed"]
    final["outcome"] = "passed" if final["qualification_eligible"] else "failed"
    final["errors"] = errors
    final["finished_unix_ns"] = time.time_ns()
    output_path.write_text(json.dumps(final, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if final["qualification_eligible"] else 1


def parse_finalize_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification", choices=("rdma", "fabric"), required=True)
    parser.add_argument("--server-root", type=Path, required=True)
    parser.add_argument("--client-root", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--expected-run-attempt", required=True)
    parser.add_argument("--expected-kinds", default="")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    kinds = [item for item in args.expected_kinds.split(",") if item]
    if args.qualification == "rdma" and kinds:
        parser.error("RDMA finalization does not accept Fabric kinds")
    if args.qualification == "fabric" and kinds != ["ipcf", "ntb", "cxl"]:
        parser.error("Fabric finalization requires --expected-kinds=ipcf,ntb,cxl")
    args.kinds = kinds
    return args


def main() -> int:
    args = parse_finalize_args()
    return finalize(
        qualification=args.qualification,
        server_root=args.server_root,
        client_root=args.client_root,
        output_path=args.out,
        policy_path=args.policy,
        expected_commit=args.expected_commit,
        expected_run_id=args.expected_run_id,
        expected_run_attempt=args.expected_run_attempt,
        expected_kinds=args.kinds,
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (QualificationError, OSError, ValueError) as error:
        print(f"transport qualification finalizer: {error}", file=sys.stderr)
        sys.exit(2)
