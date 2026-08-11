#!/usr/bin/env python3
"""Fail-closed node capacity report and D6-15 qualification runner."""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence, cast

REPORT_SCHEMA = "mino.capacity.report.v1"
INVENTORY_SCHEMA = "mino.capacity.production-inventory.v1"
SNAPSHOT_SCHEMA = "mino.coordinator.topic-snapshot.v1"
BUDGET_SCHEMA = "mino.capacity.node-budget.v1"
QUALIFICATION_ID = "D6-15"
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
U64_MAX = (1 << 64) - 1
DIMENSIONS = (
    "shm_bytes",
    "region_bytes",
    "slab_bytes",
    "topics",
    "partitions",
    "publishers",
    "subscribers",
    "bridge_lanes",
    "bridge_egress_bytes_per_second",
    "recorder_buffer_bytes",
    "recorder_disk_bytes_per_second",
    "monitoring_bytes",
    "file_descriptors",
    "threads",
)
SCHEMA_KEYS = {"type_id", "digest", "major", "minor"}
ROUTE_KEYS = {"policy", "version", "targets"}
USAGE_KEYS = {"publishers", "subscribers", "bridges", "recorders", "replay_pins"}
TOPIC_COMMON_KEYS = {
    "topic_id",
    "name",
    "channel_kind",
    "capacity",
    "max_publishers",
    "max_subscribers",
    "partition_count",
    "record_topology",
    "schema",
    "accepted_schemas",
    "route",
}
COST_KEYS = {
    "topic_overhead_bytes",
    "partition_region_overhead_bytes",
    "publisher_bytes",
    "subscriber_bytes",
    "bridge_lane_buffer_bytes",
    "monitoring_base_bytes",
    "base_file_descriptors",
    "base_threads",
    "publisher_file_descriptors",
    "subscriber_file_descriptors",
    "bridge_lane_file_descriptors",
    "recorder_partition_file_descriptors",
    "bridge_lane_threads",
    "recorder_topic_threads",
}


class ReportError(RuntimeError):
    """An input or qualification contract is invalid."""


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_sha(report: Mapping[str, Any]) -> str:
    unsigned = dict(report)
    unsigned["report_sha256"] = ""
    return hashlib.sha256(canonical_json(unsigned)).hexdigest()


def seal(report: dict[str, Any]) -> None:
    report["report_sha256"] = ""
    report["report_sha256"] = content_sha(report)


def exact_object(value: object, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReportError(f"{label} must be an object")
    actual = set(value)
    if actual != keys:
        missing = sorted(keys - actual)
        unknown = sorted(actual - keys)
        raise ReportError(f"{label} keys differ: missing={missing}, unknown={unknown}")
    return cast(dict[str, Any], value)


def array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReportError(f"{label} must be an array")
    return value


def bounded_int(value: object, label: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= U64_MAX:
        raise ReportError(f"{label} must be an integer in [{minimum}, {U64_MAX}]")
    return value


def nonempty_string(value: object, label: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise ReportError(f"{label} must be a bounded non-empty string")
    return value


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReportError(f"{label} must contain one JSON object")
    return cast(dict[str, Any], value)


def validate_schema_identity(value: object, label: str) -> dict[str, Any]:
    schema = exact_object(value, SCHEMA_KEYS, label)
    bounded_int(schema["type_id"], f"{label}.type_id", 1)
    digest = nonempty_string(schema["digest"], f"{label}.digest", 64)
    if DIGEST_RE.fullmatch(digest) is None:
        raise ReportError(f"{label}.digest must be 64 lowercase hexadecimal characters")
    bounded_int(schema["major"], f"{label}.major")
    bounded_int(schema["minor"], f"{label}.minor")
    return schema


def schema_key(schema: Mapping[str, Any]) -> tuple[object, ...]:
    return (schema["type_id"], schema["digest"], schema["major"], schema["minor"])


def validate_route(value: object, label: str) -> dict[str, Any]:
    route = exact_object(value, ROUTE_KEYS, label)
    if route["policy"] not in ("discovery", "static"):
        raise ReportError(f"{label}.policy must be discovery or static")
    bounded_int(route["version"], f"{label}.version")
    targets = array(route["targets"], f"{label}.targets")
    normalized = [bounded_int(target, f"{label}.targets[]", 1) for target in targets]
    if len(set(normalized)) != len(normalized):
        raise ReportError(f"{label}.targets contains a duplicate route target")
    if route["policy"] == "discovery" and normalized:
        raise ReportError(f"{label}: discovery route must not declare static targets")
    if route["policy"] == "static" and not normalized:
        raise ReportError(f"{label}: static route must declare at least one target")
    return route


def validate_usage(value: object, label: str) -> dict[str, Any]:
    usage = exact_object(value, USAGE_KEYS, label)
    for key in USAGE_KEYS:
        bounded_int(usage[key], f"{label}.{key}")
    return usage


def validate_topic_common(topic: dict[str, Any], label: str) -> None:
    bounded_int(topic["topic_id"], f"{label}.topic_id", 1)
    nonempty_string(topic["name"], f"{label}.name", 255)
    if topic["channel_kind"] not in ("spsc", "mpmc", "broadcast"):
        raise ReportError(f"{label}.channel_kind is unknown")
    for key in ("capacity", "max_publishers", "max_subscribers", "partition_count"):
        bounded_int(topic[key], f"{label}.{key}", 1)
    if topic["record_topology"] not in ("strong_consistent", "isolated", "best_effort"):
        raise ReportError(f"{label}.record_topology is unknown")
    primary = validate_schema_identity(topic["schema"], f"{label}.schema")
    accepted = array(topic["accepted_schemas"], f"{label}.accepted_schemas")
    identities = [schema_key(primary)]
    for index, candidate in enumerate(accepted):
        identities.append(schema_key(validate_schema_identity(candidate, f"{label}.accepted_schemas[{index}]")))
    if len(set(identities)) != len(identities):
        raise ReportError(f"{label} contains a duplicate schema identity")
    validate_route(topic["route"], f"{label}.route")


def validate_snapshot(root: dict[str, Any]) -> list[dict[str, Any]]:
    value = exact_object(root, {"schema", "generated_unix_ns", "topics"}, "snapshot")
    if value["schema"] != SNAPSHOT_SCHEMA:
        raise ReportError("Coordinator snapshot schema mismatch")
    bounded_int(value["generated_unix_ns"], "snapshot.generated_unix_ns", 1)
    topics: list[dict[str, Any]] = []
    ids: set[int] = set()
    names: set[str] = set()
    for index, candidate in enumerate(array(value["topics"], "snapshot.topics")):
        topic = exact_object(candidate, TOPIC_COMMON_KEYS | {"state", "usage"}, f"snapshot.topics[{index}]")
        validate_topic_common(topic, f"snapshot.topics[{index}]")
        if topic["state"] != "active":
            raise ReportError(f"snapshot topic {topic['name']!r} is not active")
        usage = validate_usage(topic["usage"], f"snapshot.topics[{index}].usage")
        if usage["publishers"] > topic["max_publishers"] or usage["subscribers"] > topic["max_subscribers"]:
            raise ReportError(f"snapshot topic {topic['name']!r} usage exceeds configured maxima")
        if topic["topic_id"] in ids or topic["name"] in names:
            raise ReportError("Coordinator snapshot contains a duplicate Topic ID or name")
        ids.add(topic["topic_id"])
        names.add(topic["name"])
        topics.append(topic)
    return topics


def validate_inventory(root: dict[str, Any]) -> tuple[str, bool, list[dict[str, Any]]]:
    value = exact_object(root, {"schema", "environment", "qualification_approved", "topics"}, "inventory")
    if value["schema"] != INVENTORY_SCHEMA:
        raise ReportError("production inventory schema mismatch")
    environment = nonempty_string(value["environment"], "inventory.environment", 64)
    if not isinstance(value["qualification_approved"], bool):
        raise ReportError("inventory.qualification_approved must be boolean")
    topics: list[dict[str, Any]] = []
    ids: set[int] = set()
    names: set[str] = set()
    recorder_ids: set[str] = set()
    keys = TOPIC_COMMON_KEYS | {"planning", "recorder"}
    planning_keys = {
        "message_size_bytes",
        "slab_class",
        "slab_slot_bytes",
        "peak_messages_per_second",
        "peak_publishers",
        "peak_subscribers",
        "bridge_lanes",
    }
    recorder_keys = {"recorder_id", "buffer_bytes", "max_disk_pause_seconds", "disk_bytes_per_second"}
    for index, candidate in enumerate(array(value["topics"], "inventory.topics")):
        topic = exact_object(candidate, keys, f"inventory.topics[{index}]")
        validate_topic_common(topic, f"inventory.topics[{index}]")
        planning = exact_object(topic["planning"], planning_keys, f"inventory.topics[{index}].planning")
        for key in planning_keys:
            minimum = 0 if key == "bridge_lanes" else 1
            bounded_int(planning[key], f"inventory.topics[{index}].planning.{key}", minimum)
        if planning["message_size_bytes"] > planning["slab_slot_bytes"]:
            raise ReportError(f"inventory topic {topic['name']!r} message exceeds its slab slot")
        if planning["peak_publishers"] > topic["max_publishers"] or planning["peak_subscribers"] > topic["max_subscribers"]:
            raise ReportError(f"inventory topic {topic['name']!r} peak participants exceed Coordinator maxima")
        recorder = topic["recorder"]
        if recorder is not None:
            recorder = exact_object(recorder, recorder_keys, f"inventory.topics[{index}].recorder")
            recorder_id = nonempty_string(recorder["recorder_id"], f"inventory.topics[{index}].recorder.recorder_id", 128)
            if recorder_id in recorder_ids:
                raise ReportError(f"duplicate recorder ID {recorder_id!r}")
            recorder_ids.add(recorder_id)
            for key in recorder_keys - {"recorder_id"}:
                bounded_int(recorder[key], f"inventory.topics[{index}].recorder.{key}", 1)
        if topic["topic_id"] in ids or topic["name"] in names:
            raise ReportError("production inventory contains a duplicate Topic ID or name")
        ids.add(topic["topic_id"])
        names.add(topic["name"])
        topics.append(topic)
    if not topics:
        raise ReportError("production Topic inventory is empty and must never PASS")
    return environment, value["qualification_approved"], topics


def validate_resource_vector(value: object, label: str) -> dict[str, int]:
    vector = exact_object(value, set(DIMENSIONS), label)
    return {key: bounded_int(vector[key], f"{label}.{key}") for key in DIMENSIONS}


def validate_budget(root: dict[str, Any]) -> dict[str, Any]:
    budget = exact_object(root, {"schema", "node_name", "hardware", "limits", "emergency", "slab_classes", "costs"}, "budget")
    if budget["schema"] != BUDGET_SCHEMA:
        raise ReportError("node budget schema mismatch")
    nonempty_string(budget["node_name"], "budget.node_name", 64)
    hardware = exact_object(budget["hardware"], {"profile", "architecture", "minimum_cpu_count", "minimum_memory_bytes"}, "budget.hardware")
    nonempty_string(hardware["profile"], "budget.hardware.profile", 128)
    nonempty_string(hardware["architecture"], "budget.hardware.architecture", 32)
    bounded_int(hardware["minimum_cpu_count"], "budget.hardware.minimum_cpu_count", 1)
    bounded_int(hardware["minimum_memory_bytes"], "budget.hardware.minimum_memory_bytes", 1)
    limits = validate_resource_vector(budget["limits"], "budget.limits")
    emergency = validate_resource_vector(budget["emergency"], "budget.emergency")
    for dimension in DIMENSIONS:
        if emergency[dimension] > limits[dimension]:
            raise ReportError(f"budget emergency exceeds limit for {dimension}")
    costs = exact_object(budget["costs"], COST_KEYS, "budget.costs")
    for key in COST_KEYS:
        bounded_int(costs[key], f"budget.costs.{key}")
    classes: list[dict[str, int]] = []
    class_ids: set[int] = set()
    class_keys = {"class", "slot_bytes", "limit_bytes", "emergency_bytes"}
    for index, candidate in enumerate(array(budget["slab_classes"], "budget.slab_classes")):
        slab = exact_object(candidate, class_keys, f"budget.slab_classes[{index}]")
        normalized = {key: bounded_int(slab[key], f"budget.slab_classes[{index}].{key}", 1 if key in ("slot_bytes", "limit_bytes") else 0) for key in class_keys}
        if normalized["class"] in class_ids:
            raise ReportError("budget contains a duplicate slab class")
        if normalized["emergency_bytes"] > normalized["limit_bytes"]:
            raise ReportError("slab class emergency exceeds limit")
        class_ids.add(normalized["class"])
        classes.append(normalized)
    if not classes:
        raise ReportError("budget must declare at least one slab class")
    if sum(item["limit_bytes"] for item in classes) != limits["slab_bytes"] or sum(item["emergency_bytes"] for item in classes) != emergency["slab_bytes"]:
        raise ReportError("aggregate slab limits/emergency differ from slab class budgets")
    budget["limits"] = limits
    budget["emergency"] = emergency
    budget["slab_classes"] = classes
    budget["costs"] = {key: int(costs[key]) for key in COST_KEYS}
    return budget


def run_validator(validator: Path, configs: Sequence[Path]) -> None:
    if not validator.is_file() or not os.access(validator, os.X_OK):
        raise ReportError(f"strict deployment validator is not executable: {validator}")
    with tempfile.TemporaryDirectory(prefix="mino-capacity-config-") as temporary:
        root = Path(temporary)
        for index, config in enumerate(configs):
            # mino-deploy validate intentionally includes deployment-file permission
            # checks. Bazel runfiles may point at writable source files, so feed its
            # strict TOML parser a byte-identical read-only copy. The report remains
            # bound to the original path, size, and SHA-256.
            checked = root / f"node-{index}.toml"
            checked.write_bytes(config.read_bytes())
            checked.chmod(0o400)
            completed = subprocess.run(
                [str(validator), "validate", "--config", str(checked)],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if completed.returncode != 0:
                detail = completed.stderr.strip() or completed.stdout.strip() or f"exit {completed.returncode}"
                raise ReportError(f"strict deployment config validation failed for {config}: {detail}")


def parse_toml_facts(path: Path) -> dict[str, Any]:
    section = "root"
    facts: dict[str, Any] = {}
    for number, original in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = original.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        match = re.match(r"^([A-Za-z0-9_]+)\s*=\s*(.+?)\s*(?:#.*)?$", line)
        if match is None:
            continue
        raw = match.group(2)
        if raw.startswith('"'):
            try:
                parsed: Any = json.loads(raw)
            except json.JSONDecodeError as error:
                raise ReportError(f"cannot extract deployment fact {path}:{number}: {error}") from error
        elif raw in ("true", "false"):
            parsed = raw == "true"
        else:
            try:
                parsed = int(raw)
            except ValueError:
                continue
        facts[f"{section}.{match.group(1)}"] = parsed
    required = {
        "root.schema_version",
        "node.id",
        "node.name",
        "node.environment",
        "region.bytes",
        "resources.shm_bytes",
        "resources.file_descriptors",
        "resources.threads",
        "resources.bridge_connections",
        "bridge.enabled",
        "bridge.max_connections",
        "monitoring.enabled",
        "monitoring.request_bytes_limit",
        "monitoring.response_bytes_limit",
        "monitoring.connection_limit",
        "monitoring.worker_threads",
    }
    missing = sorted(required - set(facts))
    if missing:
        raise ReportError(f"validated deployment config lacks report facts {missing}: {path}")
    return facts


def source_state(repo: Path, expected_commit: str | None, qualification: bool) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True, timeout=30).strip().lower()
        status = subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=repo, text=True, timeout=30
        ).strip()
    except (OSError, subprocess.SubprocessError) as error:
        if not qualification and expected_commit is not None and COMMIT_RE.fullmatch(expected_commit.lower()) is not None:
            commit = expected_commit.lower()
            status = "unknown"
        else:
            commit = ""
            status = "unknown"
            errors.append(f"cannot inspect source checkout: {error}")
    state = "unknown" if status == "unknown" else ("dirty" if status else "clean")
    expected = expected_commit.lower() if expected_commit else commit
    if COMMIT_RE.fullmatch(commit) is None:
        errors.append("source commit is not an exact 40-character SHA")
    if qualification:
        if expected_commit is None or COMMIT_RE.fullmatch(expected) is None:
            errors.append("qualification requires --expected-commit with an exact lowercase 40-character SHA")
        elif commit != expected:
            errors.append(f"HEAD {commit} does not match expected commit {expected}")
        if state != "clean":
            errors.append("qualification requires a clean worktree including untracked files")
    return {"expected_commit": expected, "commit": commit, "state": state}, errors


def observed_hardware() -> dict[str, Any]:
    memory = 0
    try:
        memory = int(os.sysconf("SC_PHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        pass
    return {
        "hostname": platform.node() or "unknown",
        "system": platform.system() or "unknown",
        "kernel": platform.release() or "unknown",
        "architecture": platform.machine().lower() or "unknown",
        "cpu_count": os.cpu_count() or 0,
        "memory_bytes": memory,
    }


def empty_vector() -> dict[str, int]:
    return {key: 0 for key in DIMENSIONS}


def add_vector(target: dict[str, int], addition: Mapping[str, int]) -> None:
    for key in DIMENSIONS:
        target[key] += int(addition.get(key, 0))
        if target[key] > U64_MAX:
            raise ReportError(f"resource arithmetic overflow for {key}")


def ceil_scaled(value: int, multiplier: Decimal) -> int:
    result = int((Decimal(value) * multiplier).to_integral_value(rounding=ROUND_CEILING))
    if result > U64_MAX:
        raise ReportError("what-if arithmetic exceeds uint64")
    return result


def parse_multiplier(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation as error:
        raise ReportError("peak multiplier is not a decimal") from error
    if not value.is_finite() or value < Decimal("1") or value > Decimal("1000"):
        raise ReportError("peak multiplier must be finite and in [1, 1000]")
    return value


def topic_identity(topic: Mapping[str, Any]) -> tuple[int, str]:
    return int(topic["topic_id"]), str(topic["name"])


def compare_inventory_snapshot(inventory: Sequence[dict[str, Any]], snapshot: Sequence[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    configured = {topic_identity(topic): topic for topic in inventory}
    observed = {topic_identity(topic): topic for topic in snapshot}
    missing = sorted(configured.keys() - observed.keys())
    unknown = sorted(observed.keys() - configured.keys())
    if missing:
        errors.append(f"Coordinator snapshot is missing configured production Topics: {missing}")
    if unknown:
        errors.append(f"Coordinator snapshot contains unknown production Topics: {unknown}")
    for identity in sorted(configured.keys() & observed.keys()):
        expected = configured[identity]
        actual = observed[identity]
        for key in TOPIC_COMMON_KEYS:
            if expected[key] != actual[key]:
                errors.append(f"Topic {identity[1]!r} drifted at {key}")
        expected_recorders = 1 if expected["recorder"] is not None else 0
        if actual["usage"]["recorders"] != expected_recorders:
            errors.append(f"Topic {identity[1]!r} recorder count differs from inventory")
        if actual["usage"]["bridges"] != expected["planning"]["bridge_lanes"]:
            errors.append(f"Topic {identity[1]!r} bridge lane count differs from inventory")
    return errors


def calculate(
    inventory: Sequence[dict[str, Any]],
    budget: Mapping[str, Any],
    deployment: Mapping[str, Any],
    multiplier: Decimal,
    selected: set[str],
    add_publishers: int,
    add_subscribers: int,
    add_bridge_lanes: int,
    add_partitions: int,
    disk_pause_seconds: int | None,
) -> tuple[dict[str, int], list[dict[str, Any]], dict[int, int], list[dict[str, Any]], list[str]]:
    costs = cast(Mapping[str, int], budget["costs"])
    committed = empty_vector()
    committed["file_descriptors"] = costs["base_file_descriptors"]
    committed["threads"] = costs["base_threads"]
    if deployment["monitoring.enabled"]:
        monitoring = costs["monitoring_base_bytes"] + deployment["monitoring.connection_limit"] * (
            deployment["monitoring.request_bytes_limit"] + deployment["monitoring.response_bytes_limit"]
        )
        committed["monitoring_bytes"] = monitoring
        committed["shm_bytes"] += monitoring
        committed["file_descriptors"] += deployment["monitoring.connection_limit"]
        committed["threads"] += deployment["monitoring.worker_threads"]
    class_committed: dict[int, int] = {item["class"]: 0 for item in budget["slab_classes"]}
    topics_out: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    errors: list[str] = []
    for topic in inventory:
        planning = topic["planning"]
        apply_delta = topic["name"] in selected
        partitions = topic["partition_count"] + (add_partitions if apply_delta else 0)
        publishers = ceil_scaled(planning["peak_publishers"], multiplier) + (add_publishers if apply_delta else 0)
        subscribers = ceil_scaled(planning["peak_subscribers"], multiplier) + (add_subscribers if apply_delta else 0)
        bridge_lanes = ceil_scaled(planning["bridge_lanes"], multiplier) + (add_bridge_lanes if apply_delta else 0)
        messages_per_second = ceil_scaled(planning["peak_messages_per_second"], multiplier)
        if publishers > topic["max_publishers"]:
            reason = f"what-if publishers {publishers} exceed Coordinator max_publishers {topic['max_publishers']}"
            errors.append(f"Topic {topic['name']!r}: {reason}")
            rejections.append({"kind": "topic-policy", "scope": topic["name"], "dimension": "publishers", "requested": publishers, "available": topic["max_publishers"], "reason": reason})
        if subscribers > topic["max_subscribers"]:
            reason = f"what-if subscribers {subscribers} exceed Coordinator max_subscribers {topic['max_subscribers']}"
            errors.append(f"Topic {topic['name']!r}: {reason}")
            rejections.append({"kind": "topic-policy", "scope": topic["name"], "dimension": "subscribers", "requested": subscribers, "available": topic["max_subscribers"], "reason": reason})
        if planning["slab_class"] not in class_committed:
            raise ReportError(f"Topic {topic['name']!r} uses unknown slab class {planning['slab_class']}")
        slab_budget = next(item for item in budget["slab_classes"] if item["class"] == planning["slab_class"])
        if planning["slab_slot_bytes"] != slab_budget["slot_bytes"]:
            raise ReportError(f"Topic {topic['name']!r} slab slot differs from class budget")
        partition_slab = topic["capacity"] * planning["slab_slot_bytes"]
        partition_region = partition_slab + costs["partition_region_overhead_bytes"]
        slab_bytes = partition_slab * partitions
        region_bytes = partition_region * partitions
        recorder = topic["recorder"]
        if apply_delta and disk_pause_seconds is not None and recorder is None:
            reason = "disk-pause what-if requires a configured recorder"
            errors.append(f"Topic {topic['name']!r}: {reason}")
            rejections.append({"kind": "recorder-policy", "scope": topic["name"], "dimension": "recorder_buffer_bytes", "requested": disk_pause_seconds, "available": 0, "reason": reason})
        recorder_buffer = 0
        recorder_disk = 0
        pause = 0
        recorder_record: dict[str, Any] | None = None
        if recorder is not None:
            recorder_disk = ceil_scaled(recorder["disk_bytes_per_second"], multiplier)
            minimum_disk = messages_per_second * planning["message_size_bytes"]
            if recorder_disk < minimum_disk:
                reason = f"recorder disk budget {recorder_disk} B/s is below peak payload {minimum_disk} B/s"
                errors.append(f"Topic {topic['name']!r}: {reason}")
                rejections.append({"kind": "recorder-policy", "scope": topic["name"], "dimension": "recorder_disk_bytes_per_second", "requested": minimum_disk, "available": recorder_disk, "reason": reason})
            pause = disk_pause_seconds if apply_delta and disk_pause_seconds is not None else recorder["max_disk_pause_seconds"]
            recorder_buffer = max(recorder["buffer_bytes"], recorder_disk * pause)
            recorder_record = {
                "recorder_id": recorder["recorder_id"],
                "disk_pause_seconds": pause,
                "buffer_bytes": recorder_buffer,
                "disk_bytes_per_second": recorder_disk,
            }
        vector = empty_vector()
        vector.update({
            "shm_bytes": costs["topic_overhead_bytes"] + region_bytes + publishers * costs["publisher_bytes"] + subscribers * costs["subscriber_bytes"] + bridge_lanes * costs["bridge_lane_buffer_bytes"] + recorder_buffer,
            "region_bytes": region_bytes,
            "slab_bytes": slab_bytes,
            "topics": 1,
            "partitions": partitions,
            "publishers": publishers,
            "subscribers": subscribers,
            "bridge_lanes": bridge_lanes,
            "bridge_egress_bytes_per_second": messages_per_second * planning["message_size_bytes"] * bridge_lanes,
            "recorder_buffer_bytes": recorder_buffer,
            "recorder_disk_bytes_per_second": recorder_disk,
            "file_descriptors": publishers * costs["publisher_file_descriptors"] + subscribers * costs["subscriber_file_descriptors"] + bridge_lanes * costs["bridge_lane_file_descriptors"] + (partitions * costs["recorder_partition_file_descriptors"] if recorder else 0),
            "threads": bridge_lanes * costs["bridge_lane_threads"] + (costs["recorder_topic_threads"] if recorder else 0),
        })
        add_vector(committed, vector)
        class_committed[planning["slab_class"]] += slab_bytes
        partitions_out = [
            {
                "partition": index,
                "what_if": index >= topic["partition_count"],
                "slab_class": planning["slab_class"],
                "resources": {"region_bytes": partition_region, "slab_bytes": partition_slab},
            }
            for index in range(partitions)
        ]
        topics_out.append({
            "topic_id": topic["topic_id"],
            "name": topic["name"],
            "schemas": [topic["schema"], *topic["accepted_schemas"]],
            "routes": [{"policy": topic["route"]["policy"], "version": topic["route"]["version"], "target_node": target} for target in topic["route"]["targets"]] or [{"policy": "discovery", "version": topic["route"]["version"], "target_node": None}],
            "partitions": partitions_out,
            "recorder": recorder_record,
            "resources": vector,
        })
    return committed, topics_out, class_committed, rejections, errors


def input_record(kind: str, path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    if not resolved.is_file() or resolved.stat().st_size <= 0:
        raise ReportError(f"{kind} input is missing or empty: {path}")
    return {"kind": kind, "path": str(resolved), "bytes": resolved.stat().st_size, "sha256": sha256(resolved)}


def base_report(mode: str, argv: Sequence[str]) -> dict[str, Any]:
    return {
        "schema": REPORT_SCHEMA,
        "qualification_id": QUALIFICATION_ID,
        "mode": mode,
        "outcome": "FAIL",
        "qualification_eligible": False,
        "generated_unix_ns": time.time_ns(),
        "source": {"expected_commit": "", "commit": "", "state": "unknown"},
        "hardware": {"configured": {}, "observed": {}},
        "commands": {"invocation": list(argv), "verification": ["capacity_report", "verify", "--artifact", "<report.json>"]},
        "inputs": [],
        "inventory_coverage": {"configured": [], "snapshot": [], "reported": [], "exact": False},
        "scenario": {},
        "resources": {},
        "slab_classes": [],
        "topics": [],
        "rejections": [],
        "errors": [],
        "report_sha256": "",
    }


def generate(args: argparse.Namespace, argv: Sequence[str]) -> tuple[dict[str, Any], int]:
    qualification = args.command == "qualify"
    report = base_report(args.command, argv)
    errors: list[str] = []
    try:
        repo = Path(args.repo).resolve(strict=True)
        configs = [Path(value).resolve(strict=True) for value in args.deployment_config]
        validator = Path(args.deployment_validator).resolve(strict=True)
        snapshot_path = Path(args.coordinator_snapshot).resolve(strict=True)
        inventory_path = Path(args.inventory).resolve(strict=True)
        budget_path = Path(args.budget).resolve(strict=True)
        schema_path = Path(args.contract_schema).resolve(strict=True)
        report["inputs"] = [
            *[input_record("deployment_config", path) for path in configs],
            input_record("coordinator_snapshot", snapshot_path),
            input_record("production_inventory", inventory_path),
            input_record("node_budget", budget_path),
            input_record("artifact_schema", schema_path),
        ]
        run_validator(validator, configs)
        deployments = [parse_toml_facts(path) for path in configs]
        node_ids = [item["node.id"] for item in deployments]
        node_names = [item["node.name"] for item in deployments]
        if len(set(node_ids)) != len(node_ids) or len(set(node_names)) != len(node_names):
            raise ReportError("deployment configs contain a duplicate node ID or name")
        inventory_environment, approved, inventory = validate_inventory(load_json(inventory_path, "production inventory"))
        snapshot = validate_snapshot(load_json(snapshot_path, "Coordinator snapshot"))
        budget = validate_budget(load_json(budget_path, "node budget"))
        if budget["node_name"] not in node_names:
            raise ReportError("node budget does not match any strict deployment config")
        deployment = deployments[node_names.index(budget["node_name"])]
        if inventory_environment != deployment["node.environment"]:
            raise ReportError("inventory environment differs from the budget node deployment config")
        limits = budget["limits"]
        if limits["shm_bytes"] > deployment["resources.shm_bytes"]:
            raise ReportError("capacity shm limit exceeds deployment resources.shm_bytes")
        if limits["region_bytes"] > deployment["region.bytes"]:
            raise ReportError("capacity region limit exceeds deployment region.bytes")
        if limits["file_descriptors"] > deployment["resources.file_descriptors"] or limits["threads"] > deployment["resources.threads"]:
            raise ReportError("capacity descriptor/thread limit exceeds deployment resources")
        if limits["bridge_lanes"] > deployment["resources.bridge_connections"]:
            raise ReportError("capacity bridge lane limit exceeds deployment bridge_connections")
        route_nodes = {int(node) for node in node_ids}
        for topic in inventory:
            unknown_targets = set(topic["route"]["targets"]) - route_nodes
            if unknown_targets:
                errors.append(f"Topic {topic['name']!r} route references nodes absent from deployment configs: {sorted(unknown_targets)}")
        errors.extend(compare_inventory_snapshot(inventory, snapshot))
        source, source_errors = source_state(repo, args.expected_commit, qualification)
        report["source"] = source
        errors.extend(source_errors)
        observed = observed_hardware()
        report["hardware"] = {"configured": budget["hardware"], "observed": observed}
        architecture = str(budget["hardware"]["architecture"]).lower()
        if observed["cpu_count"] < budget["hardware"]["minimum_cpu_count"]:
            errors.append("observed CPU count is below the hardware budget profile")
        if observed["memory_bytes"] and observed["memory_bytes"] < budget["hardware"]["minimum_memory_bytes"]:
            errors.append("observed physical memory is below the hardware budget profile")
        if qualification and architecture in ("", "any"):
            errors.append("qualification hardware profile must name an exact architecture")
        if architecture not in ("any", observed["architecture"]):
            errors.append(f"observed architecture {observed['architecture']!r} differs from hardware profile {architecture!r}")
        if qualification and (inventory_environment != "production" or not approved):
            errors.append("qualification requires a reviewed production inventory with qualification_approved=true")
        multiplier = parse_multiplier(args.peak_multiplier)
        selected = set(args.what_if_topic)
        inventory_names = {topic["name"] for topic in inventory}
        if selected - inventory_names:
            errors.append(f"what-if selects unknown Topics: {sorted(selected - inventory_names)}")
        delta_requested = any((args.add_publishers, args.add_subscribers, args.add_bridge_lanes, args.add_partitions)) or args.disk_pause_seconds is not None
        if delta_requested and not selected:
            errors.append("participant/lane/partition/disk-pause what-if requires --what-if-topic")
        committed, topics_out, class_committed, rejections, calculation_errors = calculate(
            inventory,
            budget,
            deployment,
            multiplier,
            selected,
            args.add_publishers,
            args.add_subscribers,
            args.add_bridge_lanes,
            args.add_partitions,
            args.disk_pause_seconds,
        )
        errors.extend(calculation_errors)
        resource_rows: dict[str, dict[str, int]] = {}
        for dimension in DIMENSIONS:
            limit = limits[dimension]
            emergency = budget["emergency"][dimension]
            ceiling = limit - emergency
            used = committed[dimension]
            resource_rows[dimension] = {"limit": limit, "committed": used, "headroom": max(0, ceiling - used), "emergency": emergency}
            if used > ceiling:
                reason = f"{dimension} committed={used} exceeds data-plane ceiling={ceiling} (limit={limit}, emergency={emergency})"
                errors.append(reason)
                rejections.append({"kind": "node-budget", "scope": budget["node_name"], "dimension": dimension, "requested": used, "available": ceiling, "reason": reason})
        slab_rows: list[dict[str, Any]] = []
        for slab in budget["slab_classes"]:
            used = class_committed[slab["class"]]
            ceiling = slab["limit_bytes"] - slab["emergency_bytes"]
            row = {**slab, "committed_bytes": used, "headroom_bytes": max(0, ceiling - used)}
            slab_rows.append(row)
            if used > ceiling:
                reason = f"slab class {slab['class']} committed={used} exceeds ceiling={ceiling}"
                errors.append(reason)
                rejections.append({"kind": "slab-budget", "scope": budget["node_name"], "dimension": f"slab_class[{slab['class']}]", "requested": used, "available": ceiling, "reason": reason})
        configured_set = [list(topic_identity(topic)) for topic in inventory]
        snapshot_set = [list(topic_identity(topic)) for topic in snapshot]
        reported_set = [list(topic_identity(topic)) for topic in topics_out]
        exact = sorted(configured_set) == sorted(snapshot_set) == sorted(reported_set)
        if not exact:
            errors.append("configured, Coordinator snapshot, and report Topic sets are not exactly equal")
        report["inventory_coverage"] = {"configured": configured_set, "snapshot": snapshot_set, "reported": reported_set, "exact": exact}
        report["scenario"] = {
            "peak_multiplier": str(multiplier),
            "topics": sorted(selected),
            "add_publishers": args.add_publishers,
            "add_subscribers": args.add_subscribers,
            "add_bridge_lanes": args.add_bridge_lanes,
            "add_partitions": args.add_partitions,
            "disk_pause_seconds": args.disk_pause_seconds,
            "accepted": not errors,
        }
        report["resources"] = resource_rows
        report["slab_classes"] = slab_rows
        report["topics"] = topics_out
        report["rejections"] = rejections
    except (OSError, ReportError, subprocess.SubprocessError) as error:
        errors.append(str(error))
    report["errors"] = errors
    report["outcome"] = "PASS" if not errors else "FAIL"
    report["qualification_eligible"] = qualification and not errors
    seal(report)
    return report, 0 if report["outcome"] == "PASS" and (not qualification or report["qualification_eligible"]) else 1


def verify(args: argparse.Namespace) -> int:
    try:
        path = Path(args.artifact).resolve(strict=True)
        report = load_json(path, "capacity report")
        required = set(base_report("report", []))
        exact_object(report, required, "capacity report")
        if report["schema"] != REPORT_SCHEMA or report["qualification_id"] != QUALIFICATION_ID:
            raise ReportError("capacity report schema or qualification ID mismatch")
        digest = report["report_sha256"]
        if not isinstance(digest, str) or DIGEST_RE.fullmatch(digest) is None or content_sha(report) != digest:
            raise ReportError("capacity report SHA-256 mismatch")
        records = array(report["inputs"], "capacity report.inputs")
        if not records:
            raise ReportError("capacity report has no hashed inputs")
        for index, candidate in enumerate(records):
            record = exact_object(candidate, {"kind", "path", "bytes", "sha256"}, f"capacity report.inputs[{index}]")
            input_path = Path(nonempty_string(record["path"], f"input[{index}].path")).resolve(strict=True)
            size = bounded_int(record["bytes"], f"input[{index}].bytes", 1)
            digest_value = nonempty_string(record["sha256"], f"input[{index}].sha256", 64)
            if DIGEST_RE.fullmatch(digest_value) is None or input_path.stat().st_size != size or sha256(input_path) != digest_value:
                raise ReportError(f"input hash/size mismatch for {record['kind']}: {input_path}")
        coverage = exact_object(report["inventory_coverage"], {"configured", "snapshot", "reported", "exact"}, "capacity report.inventory_coverage")
        if coverage["exact"] is not True or sorted(coverage["configured"]) != sorted(coverage["snapshot"]) or sorted(coverage["configured"]) != sorted(coverage["reported"]) or not coverage["configured"]:
            raise ReportError("capacity report Topic coverage is empty or not exact")
        if report["outcome"] != "PASS" or report["errors"] != [] or report["rejections"] != []:
            raise ReportError("capacity report outcome is not a clean PASS")
        if args.require_qualified:
            if report["mode"] != "qualify" or report["qualification_eligible"] is not True:
                raise ReportError("report is not qualification eligible")
            source = exact_object(report["source"], {"expected_commit", "commit", "state"}, "capacity report.source")
            expected = args.expected_commit.lower() if args.expected_commit else source["expected_commit"]
            if COMMIT_RE.fullmatch(str(expected)) is None or source != {"expected_commit": expected, "commit": expected, "state": "clean"}:
                raise ReportError("qualified report is not bound to the clean exact expected commit")
        print(f"capacity report verified: {path} sha256={digest}")
        return 0
    except (OSError, ReportError) as error:
        print(f"capacity report verification failed: {error}", file=sys.stderr)
        return 1


def add_generate_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo", default=".")
    parser.add_argument("--deployment-config", action="append", required=True)
    parser.add_argument("--deployment-validator", required=True)
    parser.add_argument("--coordinator-snapshot", required=True)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--budget", required=True)
    parser.add_argument("--contract-schema", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expected-commit")
    parser.add_argument("--peak-multiplier", default="1")
    parser.add_argument("--what-if-topic", action="append", default=[])
    parser.add_argument("--disk-pause-seconds", type=int)
    parser.add_argument("--add-publishers", type=int, default=0)
    parser.add_argument("--add-subscribers", type=int, default=0)
    parser.add_argument("--add-bridge-lanes", type=int, default=0)
    parser.add_argument("--add-partitions", type=int, default=0)


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("report", "qualify"):
        child = subparsers.add_parser(command)
        add_generate_arguments(child)
    verifier = subparsers.add_parser("verify")
    verifier.add_argument("--artifact", required=True)
    verifier.add_argument("--require-qualified", action="store_true")
    verifier.add_argument("--expected-commit")
    args = parser.parse_args(argv)
    for key in ("disk_pause_seconds", "add_publishers", "add_subscribers", "add_bridge_lanes", "add_partitions"):
        value = getattr(args, key, 0)
        if value is not None and value < 0:
            parser.error(f"--{key.replace('_', '-')} must be non-negative")
    if args.command == "qualify" and not args.expected_commit:
        parser.error("qualify requires --expected-commit")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    actual = list(sys.argv[1:] if argv is None else argv)
    args = parse(actual)
    if args.command == "verify":
        return verify(args)
    report, code = generate(args, [Path(sys.argv[0]).name, *actual])
    output = Path(args.output)
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as error:
        print(f"cannot write capacity report {output}: {error}", file=sys.stderr)
        return 1
    print(f"capacity report {report['outcome']}: {output} sha256={report['report_sha256']} qualification_eligible={str(report['qualification_eligible']).lower()}")
    for error in report["errors"]:
        print(f"capacity report error: {error}", file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
