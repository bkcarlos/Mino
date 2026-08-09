#!/usr/bin/env python3
"""Orchestrate the real Mino two-host probe and produce hashed evidence.

Python never carries application data between nodes. The subprocess must report
that TcpDriver + BridgeConnectionManager/BridgePipeline completed discovery,
reliable delivery, and the remote ACK.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import platform
import re
import signal
import subprocess
import tempfile
import time
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROTOCOL = "mino-two-host-mino-v2"
SCHEMA_VERSION = 6
MINO_RESULT_SCHEMA_VERSION = 6
MIN_TOKEN_BYTES = 32
MIN_PRODUCTION_TIMEOUT_SECONDS = 1800
MAX_TIMEOUT_SECONDS = 3600
COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
HOST_RE = re.compile(
    r"^(?=.{1,253}$)(?!-)(?:[A-Za-z0-9-]{1,63}\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)
TOPIC_ROUTE_KEYS = frozenset(
    {
        "topic_id",
        "direction",
        "messages",
        "ordered",
        "payload_verified",
        "cross_topic_leakage",
    }
)
NETWORK_SCALING_KEYS = frozenset(
    {
        "mode",
        "topic_count",
        "messages_per_topic",
        "messages_sent",
        "messages_received",
        "application_payload_bytes",
        "mino_frame_body_bytes",
        "pipeline_inbound_frames",
        "pipeline_outbound_frames",
        "tcp_prefix_bytes",
        "tcp_framed_bytes",
        "elapsed_ms",
        "accepted_acks",
        "retransmissions",
        "duplicate_suppressed",
        "cross_topic_leakage",
    }
)
CORRECTNESS_MESSAGES_PER_TOPIC = 64
NETWORK_SCALING_MODES = ("fixed_total", "fixed_per_topic")
NETWORK_SCALING_TOPIC_COUNTS = (1, 2, 4, 8, 16, 32)
NETWORK_SCALING_FIXED_TOTAL_MESSAGES = 384
NETWORK_SCALING_FIXED_PER_TOPIC_MESSAGES = 32
NETWORK_SCALING_PAYLOAD_BYTES = 256
NETWORK_SCALING_POSITIVE_INTEGER_FIELDS = (
    "topic_count",
    "messages_per_topic",
    "messages_sent",
    "messages_received",
    "application_payload_bytes",
    "mino_frame_body_bytes",
    "pipeline_inbound_frames",
    "pipeline_outbound_frames",
    "tcp_prefix_bytes",
    "tcp_framed_bytes",
    "elapsed_ms",
    "accepted_acks",
)
NETWORK_SCALING_NONNEGATIVE_INTEGER_FIELDS = (
    "retransmissions",
    "duplicate_suppressed",
)
LATENCY_SAMPLE_KEYS = frozenset(
    {
        "topic_count",
        "messages_per_topic",
        "sample_count",
        "single_message_rtt_us",
        "p50_rtt_us",
        "p95_rtt_us",
        "p99_rtt_us",
        "max_rtt_us",
    }
)
LATENCY_TOPIC_COUNTS = (1, 2, 4, 8, 16, 32)
LATENCY_MESSAGES_PER_TOPIC = 64
LANE_CONNECTION_KEYS = frozenset(
    {
        "lane_index",
        "active",
        "local_session_epoch",
        "remote_session_epoch",
        "connection_attempts",
        "accepted_connections",
        "completed_handshakes",
        "reconnects",
        "disconnects",
    }
)
LATENCY_RTT_FIELDS = (
    "single_message_rtt_us",
    "p50_rtt_us",
    "p95_rtt_us",
    "p99_rtt_us",
    "max_rtt_us",
)
REQUIRED_MINO_TRUE_FIELDS = (
    "session_discovery",
    "bridge_active",
    "reliable_sent",
    "reliable_received",
    "remote_acknowledged",
    "schema_identity_nonempty",
    "descriptor_artifact_nonempty",
    "schema_announcement",
    "schema_request",
    "schema_persisted",
    "persisted_schema_identity_verified",
    "persisted_schema_bytes_verified",
    "forced_disconnect",
    "automatic_reconnect",
    "session_epoch_changed",
    "pending_reliable_before_disconnect",
    "pending_reliable_recovered",
    "pending_reliable_retransmitted",
    "reliable_replay_sent",
    "reliable_replay_pending_observed",
    "dedup_state_preserved",
    "duplicate_suppressed",
    "bidirectional_ack",
)


class ProtocolError(RuntimeError):
    """Invalid orchestration input or untrusted Mino probe output."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class EventLog:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        self.path: Path = path
        self._output = os.fdopen(descriptor, "w", encoding="utf-8", buffering=1)

    def event(self, name: str, **fields: Any) -> None:
        self._output.write(
            json.dumps({"at": utc_now(), "event": name, **fields}, sort_keys=True)
            + "\n"
        )

    def close(self) -> None:
        if not self._output.closed:
            self._output.flush()
            os.fsync(self._output.fileno())
            self._output.close()


def load_token(environment: Mapping[str, str], variable: str) -> bytes:
    raw = environment.get(variable)
    if raw is None:
        raise ProtocolError(f"required token environment variable {variable} is unset")
    token = raw.encode()
    if len(token) < MIN_TOKEN_BYTES:
        raise ProtocolError(
            f"{variable} must contain at least {MIN_TOKEN_BYTES} UTF-8 bytes"
        )
    return token


def resolve_commit(workspace: Path, explicit: str | None = None) -> str:
    candidates: list[str | None] = [explicit, os.environ.get("GITHUB_SHA")]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=workspace,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
        if result.returncode == 0:
            candidates.append(result.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    for candidate in candidates:
        if candidate and COMMIT_RE.fullmatch(candidate):
            return candidate.lower()
    raise ProtocolError("a 40-hex git commit is required (--commit or checked-out git HEAD)")


def validate_host(value: str, *, allow_loopback: bool = False) -> str:
    if not value or any(character.isspace() for character in value):
        raise ProtocolError("address must be a host name or IP without whitespace")
    candidate = value.strip("[]")
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        if not HOST_RE.fullmatch(candidate):
            raise ProtocolError(f"invalid host name: {value!r}")
        if candidate.lower() == "localhost" and not allow_loopback:
            raise ProtocolError("loopback endpoint is forbidden for physical two-host CI")
        return candidate
    if address.is_unspecified or address.is_multicast:
        raise ProtocolError("advertised endpoint cannot be unspecified or multicast")
    if address.is_loopback and not allow_loopback:
        raise ProtocolError("loopback endpoint is forbidden for physical two-host CI")
    return address.compressed


def validate_bind_host(value: str) -> str:
    candidate = value.strip("[]")
    try:
        return ipaddress.ip_address(candidate).compressed
    except ValueError:
        if not HOST_RE.fullmatch(candidate):
            raise ProtocolError(f"invalid bind address: {value!r}")
        return candidate


def validate_port(value: int) -> int:
    if not 1024 <= value <= 65535:
        raise ProtocolError("port must be between 1024 and 65535")
    return value


def validate_timeout(value: int, *, allow_short: bool = False) -> int:
    minimum = 1 if allow_short else MIN_PRODUCTION_TIMEOUT_SECONDS
    if not minimum <= value <= MAX_TIMEOUT_SECONDS:
        raise ProtocolError(
            f"timeout must be between {minimum} and {MAX_TIMEOUT_SECONDS} seconds"
        )
    return value


def format_endpoint(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _machine_material() -> bytes:
    for candidate in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
        try:
            value = candidate.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return f"machine-id:{value}".encode()
    return f"fallback:{platform.node()}:{uuid.getnode():012x}".encode()


def machine_identity(token: bytes) -> str:
    return hmac.new(
        token, b"mino-two-host-machine-v2\0" + _machine_material(), hashlib.sha256
    ).hexdigest()


def run_proof(token: bytes, commit: str, run_nonce: str) -> str:
    if not run_nonce or len(run_nonce) > 256 or "|" in run_nonce:
        raise ProtocolError("run nonce is missing or invalid")
    context = commit.encode() + b"\0" + run_nonce.encode()
    return hmac.new(token, b"mino-two-host-run-v2\0" + context, hashlib.sha256).hexdigest()


def _json_object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _load_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(f"cannot read {description} {path}: {error}") from error
    if not isinstance(decoded, dict):
        raise ProtocolError(f"{description} root is not a JSON object")
    return decoded


def _terminate(process: subprocess.Popen[str], log: EventLog) -> None:
    if process.poll() is not None:
        return
    log.event("terminate_mino_probe", pid=process.pid)
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def _require_integer(value: Any, *, description: str, minimum: int = 1) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        if minimum == 0:
            qualifier = "nonnegative integer"
        elif minimum == 1:
            qualifier = "positive integer"
        else:
            qualifier = f"integer of at least {minimum}"
        raise ProtocolError(f"{description} is not a {qualifier}")
    return value


def _validate_topic_routes(value: Any, *, role: str) -> dict[tuple[int, str], int]:
    if not isinstance(value, list) or len(value) != 4:
        raise ProtocolError("Mino result 'topic_routes' is not an array of four records")
    expected = {
        (1001, "sent"),
        (1002, "sent"),
        (2001, "received"),
        (2002, "received"),
    }
    if role == "client":
        expected = {
            (topic_id, "received" if direction == "sent" else "sent")
            for topic_id, direction in expected
        }
    indexed: dict[tuple[int, str], int] = {}
    for index, raw_record in enumerate(value):
        if (
            not isinstance(raw_record, dict)
            or frozenset(raw_record) != TOPIC_ROUTE_KEYS
        ):
            raise ProtocolError(
                f"Mino topic route {index} does not contain exactly the required keys"
            )
        topic_id = _require_integer(
            raw_record["topic_id"], description=f"Mino topic route {index} topic_id"
        )
        direction = raw_record["direction"]
        if direction not in ("sent", "received"):
            raise ProtocolError(f"Mino topic route {index} direction is invalid")
        messages = _require_integer(
            raw_record["messages"],
            description=f"Mino topic route {index} messages",
            minimum=CORRECTNESS_MESSAGES_PER_TOPIC,
        )
        if messages != CORRECTNESS_MESSAGES_PER_TOPIC:
            raise ProtocolError(
                f"Mino topic route {index} correctness message count is not "
                f"{CORRECTNESS_MESSAGES_PER_TOPIC}"
            )
        if raw_record["ordered"] is not True:
            raise ProtocolError(f"Mino topic route {index} is not ordered")
        if raw_record["payload_verified"] is not True:
            raise ProtocolError(f"Mino topic route {index} payload is not verified")
        if raw_record["cross_topic_leakage"] is not False:
            raise ProtocolError(f"Mino topic route {index} reports cross-topic leakage")
        key = (topic_id, direction)
        if key in indexed:
            raise ProtocolError(f"Mino topic route {index} duplicates {key!r}")
        indexed[key] = messages
    if set(indexed) != expected:
        raise ProtocolError(f"Mino topic routes do not match the expected {role} routes")
    return indexed


def _validate_network_scaling(
    value: Any,
) -> dict[tuple[str, int], Mapping[str, Any]]:
    if not isinstance(value, list) or len(value) != 12:
        raise ProtocolError(
            "Mino result 'network_scaling' is not an array of twelve records"
        )
    expected = {
        (mode, topic_count)
        for mode in NETWORK_SCALING_MODES
        for topic_count in NETWORK_SCALING_TOPIC_COUNTS
    }
    indexed: dict[tuple[str, int], Mapping[str, Any]] = {}
    for index, raw_record in enumerate(value):
        if (
            not isinstance(raw_record, dict)
            or frozenset(raw_record) != NETWORK_SCALING_KEYS
        ):
            raise ProtocolError(
                f"Mino network scaling record {index} does not contain exactly the required keys"
            )
        mode = raw_record["mode"]
        if mode not in NETWORK_SCALING_MODES:
            raise ProtocolError(f"Mino network scaling record {index} mode is invalid")
        for field in NETWORK_SCALING_POSITIVE_INTEGER_FIELDS:
            _require_integer(
                raw_record[field],
                description=f"Mino network scaling record {index} {field}",
            )
        for field in NETWORK_SCALING_NONNEGATIVE_INTEGER_FIELDS:
            _require_integer(
                raw_record[field],
                description=f"Mino network scaling record {index} {field}",
                minimum=0,
            )
        if raw_record["cross_topic_leakage"] is not False:
            raise ProtocolError(
                f"Mino network scaling record {index} reports cross-topic leakage"
            )
        expected_messages_per_topic = (
            NETWORK_SCALING_FIXED_TOTAL_MESSAGES // raw_record["topic_count"]
            if mode == "fixed_total"
            else NETWORK_SCALING_FIXED_PER_TOPIC_MESSAGES
        )
        if raw_record["messages_per_topic"] != expected_messages_per_topic:
            raise ProtocolError(
                f"Mino network scaling record {index} does not match the required workload"
            )
        expected_messages = (
            raw_record["topic_count"] * expected_messages_per_topic
        )
        if (
            raw_record["messages_sent"] != expected_messages
            or raw_record["messages_received"] != expected_messages
        ):
            raise ProtocolError(
                f"Mino network scaling record {index} message counts are inconsistent"
            )
        expected_prefix_bytes = 4 * (
            raw_record["pipeline_inbound_frames"]
            + raw_record["pipeline_outbound_frames"]
        )
        if raw_record["tcp_prefix_bytes"] != expected_prefix_bytes:
            raise ProtocolError(
                f"Mino network scaling record {index} TCP prefix bytes are inconsistent"
            )
        if raw_record["tcp_framed_bytes"] != (
            raw_record["mino_frame_body_bytes"] + raw_record["tcp_prefix_bytes"]
        ):
            raise ProtocolError(
                f"Mino network scaling record {index} TCP framed bytes are inconsistent"
            )
        if raw_record["accepted_acks"] < raw_record["messages_sent"]:
            raise ProtocolError(
                f"Mino network scaling record {index} accepted ACK count is too small"
            )
        if raw_record["application_payload_bytes"] != (
            raw_record["messages_sent"] * NETWORK_SCALING_PAYLOAD_BYTES
        ):
            raise ProtocolError(
                f"Mino network scaling record {index} payload bytes do not match "
                f"{NETWORK_SCALING_PAYLOAD_BYTES} bytes per message"
            )
        key = (mode, raw_record["topic_count"])
        if key in indexed:
            raise ProtocolError(
                f"Mino network scaling record {index} duplicates {key!r}"
            )
        indexed[key] = raw_record
    if set(indexed) != expected:
        raise ProtocolError(
            "Mino network scaling records are not the required Cartesian set"
        )
    fixed_total_messages = {
        record["messages_sent"]
        for (mode, _), record in indexed.items()
        if mode == "fixed_total"
    }
    if len(fixed_total_messages) != 1:
        raise ProtocolError("Mino fixed-total scaling workload is not constant")
    fixed_per_topic_messages = {
        record["messages_per_topic"]
        for (mode, _), record in indexed.items()
        if mode == "fixed_per_topic"
    }
    if len(fixed_per_topic_messages) != 1:
        raise ProtocolError("Mino fixed-per-topic scaling workload is not constant")
    payload_bytes_per_message = {
        record["application_payload_bytes"] // record["messages_sent"]
        for record in indexed.values()
    }
    if len(payload_bytes_per_message) != 1:
        raise ProtocolError("Mino network scaling payload size is not constant")
    return indexed


def _validate_latency_samples(
    value: Any,
) -> dict[int, Mapping[str, Any]]:
    if not isinstance(value, list) or len(value) != len(LATENCY_TOPIC_COUNTS):
        raise ProtocolError("Mino result 'latency_samples' is not an array of six records")
    indexed: dict[int, Mapping[str, Any]] = {}
    for index, raw_record in enumerate(value):
        if (
            not isinstance(raw_record, dict)
            or frozenset(raw_record) != LATENCY_SAMPLE_KEYS
        ):
            raise ProtocolError(
                f"Mino latency sample {index} does not contain exactly the required keys"
            )
        for field in LATENCY_SAMPLE_KEYS:
            _require_integer(
                raw_record[field],
                description=f"Mino latency sample {index} {field}",
            )
        topic_count = raw_record["topic_count"]
        if raw_record["messages_per_topic"] != LATENCY_MESSAGES_PER_TOPIC:
            raise ProtocolError(
                f"Mino latency sample {index} messages_per_topic is not "
                f"{LATENCY_MESSAGES_PER_TOPIC}"
            )
        if raw_record["sample_count"] != topic_count * LATENCY_MESSAGES_PER_TOPIC:
            raise ProtocolError(
                f"Mino latency sample {index} sample_count is inconsistent"
            )
        if not (
            raw_record["p50_rtt_us"]
            <= raw_record["p95_rtt_us"]
            <= raw_record["p99_rtt_us"]
            <= raw_record["max_rtt_us"]
        ):
            raise ProtocolError(
                f"Mino latency sample {index} percentile order is invalid"
            )
        if raw_record["single_message_rtt_us"] > raw_record["max_rtt_us"]:
            raise ProtocolError(
                f"Mino latency sample {index} single-message RTT exceeds max RTT"
            )
        if topic_count in indexed:
            raise ProtocolError(
                f"Mino latency sample {index} duplicates topic_count {topic_count}"
            )
        indexed[topic_count] = raw_record
    if set(indexed) != set(LATENCY_TOPIC_COUNTS):
        raise ProtocolError("Mino latency samples do not contain the required topic counts")
    return indexed


def _validate_mino_result(
    result: Mapping[str, Any],
    *,
    role: str,
    commit: str,
    local_identity: str,
    tcp_lane_count: int,
) -> None:
    if not isinstance(tcp_lane_count, int) or isinstance(tcp_lane_count, bool) or not 1 <= tcp_lane_count <= 8:
        raise ProtocolError("Mino expected TCP lane count is invalid")
    expected_scalars = {
        "schema_version": MINO_RESULT_SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "role": role,
        "outcome": "passed",
        "commit": commit,
        "machine_identity": local_identity,
    }
    for field, expected in expected_scalars.items():
        if result.get(field) != expected:
            raise ProtocolError(f"Mino result field {field!r} is not {expected!r}")
    peer_commit = result.get("peer_commit")
    peer_identity = result.get("peer_machine_identity")
    if peer_commit != commit:
        raise ProtocolError("Mino peer commit differs from the checked-out commit")
    if not isinstance(peer_identity, str) or not DIGEST_RE.fullmatch(peer_identity):
        raise ProtocolError("Mino peer machine identity is invalid")
    if hmac.compare_digest(peer_identity, local_identity):
        raise ProtocolError("server and client machine identities are equal")
    for field in REQUIRED_MINO_TRUE_FIELDS:
        if result.get(field) is not True:
            raise ProtocolError(f"Mino data-path evidence {field!r} is not true")
    if result.get("tcp_lane_count") != tcp_lane_count:
        raise ProtocolError("Mino result TCP lane count differs from requested config")
    if result.get("active_lane_connections") != tcp_lane_count:
        raise ProtocolError("Mino result did not activate every configured TCP lane")
    if result.get("exercised_lane_count") != tcp_lane_count:
        raise ProtocolError("Mino workload did not exercise every configured TCP lane")
    lane_connections = result.get("lane_connections")
    if not isinstance(lane_connections, list) or len(lane_connections) != tcp_lane_count:
        raise ProtocolError("Mino lane connection evidence has invalid cardinality")
    for lane_index, lane in enumerate(lane_connections):
        if not isinstance(lane, Mapping) or frozenset(lane) != LANE_CONNECTION_KEYS:
            raise ProtocolError("Mino lane connection evidence has invalid fields")
        if lane.get("lane_index") != lane_index or lane.get("active") is not True:
            raise ProtocolError("Mino lane connection is not active or canonically ordered")
        for field in ("local_session_epoch", "remote_session_epoch", "completed_handshakes"):
            value = lane.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ProtocolError(f"Mino lane field {field!r} is not positive")
        for field in ("connection_attempts", "accepted_connections", "reconnects", "disconnects"):
            value = lane.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ProtocolError(f"Mino lane field {field!r} is negative")
        role_connections = lane["accepted_connections" if role == "server" else "connection_attempts"]
        if role_connections <= 0:
            raise ProtocolError("Mino lane never established its role-specific connection")
    _validate_topic_routes(result.get("topic_routes"), role=role)
    _validate_network_scaling(result.get("network_scaling"))
    _validate_latency_samples(result.get("latency_samples"))
    for field in (
        "initial_local_session_epoch",
        "initial_remote_session_epoch",
        "local_session_epoch",
        "remote_session_epoch",
    ):
        value = result.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ProtocolError(f"Mino result {field!r} is not positive")
    if result["initial_local_session_epoch"] == result["local_session_epoch"]:
        raise ProtocolError("Mino local session epoch did not change")
    if result["initial_remote_session_epoch"] == result["remote_session_epoch"]:
        raise ProtocolError("Mino remote session epoch did not change")
    minimum_counters = {
        "completed_handshakes": 3,
        "reconnects": 2,
        "disconnects": 2,
        "accepted_acks": 3,
        "duplicate_checks": 1,
        "descriptor_authentications": 1,
        "descriptor_persistences": 1,
        "descriptor_artifact_bytes": 1,
        "accepted_connections" if role == "server" else "connection_attempts": 3,
    }
    for field, minimum in minimum_counters.items():
        value = result.get(field)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < minimum
        ):
            raise ProtocolError(
                f"Mino result {field!r} is below required minimum {minimum}"
            )
    for field in (
        "local_schema_digest",
        "peer_schema_digest",
        "persisted_schema_digest",
    ):
        value = result.get(field)
        if not isinstance(value, str) or DIGEST_RE.fullmatch(value) is None:
            raise ProtocolError(f"Mino result {field!r} is not a SHA-256 digest")
    if hmac.compare_digest(
        result["local_schema_digest"], result["peer_schema_digest"]
    ):
        raise ProtocolError("local and peer probe schemas are unexpectedly identical")
    if not hmac.compare_digest(
        result["persisted_schema_digest"], result["peer_schema_digest"]
    ):
        raise ProtocolError("persisted schema digest does not match peer schema")


def run_role(
    *,
    role: str,
    binary: Path,
    address: str,
    advertise_address: str,
    port: int,
    timeout_seconds: int,
    token: bytes,
    commit: str,
    manifest_path: Path,
    log_path: Path,
    tcp_lane_count: int = 1,
    allow_short_timeout: bool = False,
    allow_loopback: bool = False,
    identity_override: str | None = None,
) -> int:
    if role not in ("server", "client"):
        raise ProtocolError("role must be server or client")
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ProtocolError(f"Mino probe binary is not executable: {binary}")
    address = (
        validate_bind_host(address)
        if role == "server"
        else validate_host(address, allow_loopback=allow_loopback)
    )
    advertise_address = validate_host(
        advertise_address, allow_loopback=allow_loopback
    )
    port = validate_port(port)
    if not isinstance(tcp_lane_count, int) or isinstance(tcp_lane_count, bool) or not 1 <= tcp_lane_count <= 8:
        raise ProtocolError("TCP lane count must be an integer from 1 through 8")
    timeout_seconds = validate_timeout(timeout_seconds, allow_short=allow_short_timeout)
    local_identity = identity_override or machine_identity(token)
    if not DIGEST_RE.fullmatch(local_identity):
        raise ProtocolError("local machine identity override is invalid")
    run_nonce = (
        os.environ.get("MINO_TWO_HOST_RUN_NONCE")
        or os.environ.get("GITHUB_RUN_ID")
        or "local-manual-run"
    )
    proof = run_proof(token, commit, run_nonce)
    binary_hash = sha256_file(binary)
    result_path = manifest_path.parent / "mino-result.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.unlink(missing_ok=True)
    log = EventLog(log_path)
    command = [
        str(binary),
        f"--role={role}",
        f"--address={address}",
        f"--advertise-address={advertise_address}",
        f"--port={port}",
        f"--deadline-seconds={timeout_seconds}",
        f"--tcp-lane-count={tcp_lane_count}",
        f"--commit={commit}",
        f"--machine-identity={local_identity}",
        f"--run-proof={proof}",
        f"--output={result_path}",
    ]
    started_at = utc_now()
    started = time.monotonic()
    error: BaseException | None = None
    mino_result: dict[str, Any] = {}
    schema_artifacts: list[dict[str, Any]] = []
    return_code: int | None = None
    try:
        log.event(
            "start_mino_probe",
            role=role,
            binary_sha256=binary_hash,
            address=address,
            advertise_address=advertise_address,
            timeout_seconds=timeout_seconds,
            tcp_lane_count=tcp_lane_count,
        )
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
        )
        try:
            output, _ = process.communicate(timeout=timeout_seconds + 10)
        except subprocess.TimeoutExpired as timeout_error:
            _terminate(process, log)
            raise TimeoutError("Mino probe exceeded orchestrator deadline") from timeout_error
        return_code = process.returncode
        log.event("mino_probe_output", return_code=return_code, output=output.strip())
        mino_result = _load_json_object(result_path, "Mino result")
        if return_code != 0:
            raise ProtocolError(
                f"Mino probe exited {return_code}: {mino_result.get('error')}"
            )
        _validate_mino_result(
            mino_result,
            role=role,
            commit=commit,
            local_identity=local_identity,
            tcp_lane_count=tcp_lane_count,
        )
        schema_directory = result_path.parent / "schema-store" / "schemas"
        persisted_paths = sorted(schema_directory.glob("*.schema"))
        expected_schema_digest = mino_result["peer_schema_digest"]
        expected_schema_path = schema_directory / f"{expected_schema_digest}.schema"
        if persisted_paths != [expected_schema_path]:
            raise ProtocolError(
                "Mino probe persisted schema set does not exactly match peer digest"
            )
        for persisted_path in persisted_paths:
            if not persisted_path.is_file() or persisted_path.stat().st_size <= 0:
                raise ProtocolError(
                    f"persisted schema artifact is missing or empty: {persisted_path}"
                )
            schema_artifacts.append(
                {
                    "path": str(persisted_path.relative_to(manifest_path.parent)),
                    "canonical_digest": expected_schema_digest,
                    "identity_verified": mino_result[
                        "persisted_schema_identity_verified"
                    ],
                    "bytes_verified": mino_result[
                        "persisted_schema_bytes_verified"
                    ],
                    "sha256": sha256_file(persisted_path),
                    "size_bytes": persisted_path.stat().st_size,
                }
            )
    except BaseException as caught:
        error = caught
        log.event("role_failed", error_type=type(caught).__name__, error=str(caught))
    finished_at = utc_now()
    log.close()
    peer_identity = mino_result.get("peer_machine_identity")
    peer_commit = mino_result.get("peer_commit")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "role": role,
        "outcome": "passed" if error is None else "failed",
        "commit": commit,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "local": {
            "identity": local_identity,
            "advertised_address": format_endpoint(advertise_address, port),
        },
        "peer": {
            "commit": peer_commit,
            "identity": peer_identity,
            "advertised_address": mino_result.get("peer_address"),
            "configured_address": format_endpoint(address, port),
        },
        "mino": mino_result,
        "mino_result_artifact": {
            "path": result_path.name,
            "sha256": sha256_file(result_path) if result_path.is_file() else None,
            "size_bytes": result_path.stat().st_size if result_path.is_file() else 0,
        },
        "schema_artifacts": schema_artifacts,
        "binary": {
            "path": binary.name,
            "sha256": binary_hash,
            "size_bytes": binary.stat().st_size,
        },
        "process_exit_code": return_code,
        "github": github_provenance(os.environ),
        "error": None if error is None else f"{type(error).__name__}: {error}",
        "log": {
            "path": log.path.name,
            "sha256": sha256_file(log.path),
            "size_bytes": log.path.stat().st_size,
        },
    }
    write_json_atomic(manifest_path, manifest)
    print(f"{role}_manifest={manifest_path}", flush=True)
    return 0 if error is None else 1


def github_provenance(environment: Mapping[str, str]) -> dict[str, str | None]:
    return {
        name.lower(): environment.get(name)
        for name in (
            "GITHUB_ACTIONS",
            "GITHUB_REPOSITORY",
            "GITHUB_RUN_ID",
            "GITHUB_RUN_ATTEMPT",
            "GITHUB_WORKFLOW",
            "GITHUB_SHA",
        )
    }


def _validate_build_root(
    root: Path,
    *,
    role: str,
    expected_commit: str | None,
    expected_run_id: str | None,
    expected_run_attempt: str | None,
) -> dict[str, Any]:
    if root.is_symlink() or not root.is_dir():
        raise ProtocolError(f"{role} hosted build root is missing or unsafe")
    binary_path = root / "mino_two_host_probe"
    hash_path = root / "mino_two_host_probe.sha256"
    provenance_path = root / "provenance.json"
    commit_path = root / "commit.txt"
    for path in (binary_path, hash_path, provenance_path, commit_path):
        if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0:
            raise ProtocolError(f"{role} hosted build file is missing or unsafe: {path}")
    binary_hash = sha256_file(binary_path)
    hash_lines = hash_path.read_text(encoding="ascii").splitlines()
    expected_hash_line = f"{binary_hash}  mino_two_host_probe"
    if hash_lines != [expected_hash_line]:
        raise ProtocolError(f"{role} hosted build hash record is invalid")
    commit = commit_path.read_text(encoding="ascii").strip()
    provenance = _load_json_object(provenance_path, f"{role} build provenance")
    if commit != provenance.get("commit"):
        raise ProtocolError(f"{role} build commit and provenance differ")
    if expected_commit is not None and commit != expected_commit.lower():
        raise ProtocolError(f"{role} hosted build commit differs from expected commit")
    if expected_run_id is not None and provenance.get("run_id") != expected_run_id:
        raise ProtocolError(f"{role} hosted build run ID differs from expected run")
    if (
        expected_run_attempt is not None
        and provenance.get("run_attempt") != expected_run_attempt
    ):
        raise ProtocolError(
            f"{role} hosted build run attempt differs from expected attempt"
        )
    return {
        "binary_sha256": binary_hash,
        "binary_size_bytes": binary_path.stat().st_size,
        "commit": commit,
        "hash_record": {
            "sha256": sha256_file(hash_path),
            "size_bytes": hash_path.stat().st_size,
        },
        "provenance": provenance,
        "provenance_artifact": {
            "sha256": sha256_file(provenance_path),
            "size_bytes": provenance_path.stat().st_size,
        },
    }


def finalize_evidence(
    *,
    server_manifest_path: Path,
    client_manifest_path: Path,
    server_build_root: Path,
    client_build_root: Path,
    output_path: Path,
    expected_commit: str | None = None,
    expected_run_id: str | None = None,
    expected_run_attempt: str | None = None,
) -> int:
    errors: list[str] = []
    builds: dict[str, dict[str, Any]] = {}
    for role, root in (("server", server_build_root), ("client", client_build_root)):
        try:
            builds[role] = _validate_build_root(
                root,
                role=role,
                expected_commit=expected_commit,
                expected_run_id=expected_run_id,
                expected_run_attempt=expected_run_attempt,
            )
        except ProtocolError as error:
            errors.append(str(error))
    roles: dict[str, dict[str, Any]] = {}
    for role, path in (
        ("server", server_manifest_path),
        ("client", client_manifest_path),
    ):
        try:
            roles[role] = _load_json_object(path, f"{role} manifest")
        except ProtocolError as error:
            errors.append(str(error))
    server = roles.get("server", {})
    client = roles.get("client", {})
    for role, manifest in (("server", server), ("client", client)):
        if (
            manifest.get("schema_version") != SCHEMA_VERSION
            or manifest.get("protocol") != PROTOCOL
            or manifest.get("role") != role
        ):
            errors.append(f"{role} manifest schema, protocol, or role mismatch")
        if manifest.get("outcome") != "passed":
            errors.append(f"{role} outcome is not passed")
        if manifest.get("process_exit_code") != 0:
            errors.append(f"{role} Mino probe exit code is not zero")
        role_github = _json_object(manifest.get("github"))
        if expected_run_id is not None and role_github.get("github_run_id") != expected_run_id:
            errors.append(f"{role} GitHub run ID differs from expected run")
        if (
            expected_run_attempt is not None
            and role_github.get("github_run_attempt") != expected_run_attempt
        ):
            errors.append(f"{role} GitHub run attempt differs from expected attempt")

    server_local = _json_object(server.get("local"))
    server_peer = _json_object(server.get("peer"))
    client_local = _json_object(client.get("local"))
    client_peer = _json_object(client.get("peer"))
    server_binary = _json_object(server.get("binary"))
    client_binary = _json_object(client.get("binary"))
    server_mino = _json_object(server.get("mino"))
    client_mino = _json_object(client.get("mino"))
    server_commit = server.get("commit")
    client_commit = client.get("commit")
    server_github = _json_object(server.get("github"))
    client_github = _json_object(client.get("github"))
    validated_results: set[str] = set()
    for role, result, role_commit, local, peer in (
        ("server", server_mino, server_commit, server_local, server_peer),
        ("client", client_mino, client_commit, client_local, client_peer),
    ):
        local_identity = local.get("identity")
        if not isinstance(role_commit, str) or not isinstance(local_identity, str):
            errors.append(f"{role} manifest commit or local identity is invalid")
        else:
            try:
                _validate_mino_result(
                    result,
                    role=role,
                    commit=role_commit,
                    local_identity=local_identity,
                    tcp_lane_count=result.get("tcp_lane_count"),
                )
            except ProtocolError as error:
                errors.append(f"{role} nested Mino result: {error}")
            else:
                validated_results.add(role)
        if result.get("local_address") != local.get("advertised_address"):
            errors.append(f"{role} Mino local address differs from role manifest")
        if result.get("peer_commit") != peer.get("commit"):
            errors.append(f"{role} Mino peer commit differs from role manifest")
        if result.get("peer_machine_identity") != peer.get("identity"):
            errors.append(f"{role} Mino peer identity differs from role manifest")
        if result.get("peer_address") != peer.get("advertised_address"):
            errors.append(f"{role} Mino peer address differs from role manifest")

    if server_mino.get("tcp_lane_count") != client_mino.get("tcp_lane_count"):
        errors.append("server and client TCP lane counts differ")
    if server_github.get("github_run_id") != client_github.get("github_run_id"):
        errors.append("server and client GitHub run IDs differ")
    if server_github.get("github_run_attempt") != client_github.get("github_run_attempt"):
        errors.append("server and client GitHub run attempts differ")
    for role, role_commit, provenance in (
        ("server", server_commit, server_github),
        ("client", client_commit, client_github),
    ):
        github_sha = provenance.get("github_sha")
        if github_sha is not None and github_sha != role_commit:
            errors.append(f"{role} GitHub SHA differs from role commit")
    if expected_commit:
        expected_commit = expected_commit.lower()
        if server_commit != expected_commit or client_commit != expected_commit:
            errors.append("one or both role commits differ from expected commit")
    if server_peer.get("commit") != client_commit or server_mino.get(
        "peer_commit"
    ) != client_commit:
        errors.append("server Mino payload did not corroborate client commit")
    if client_peer.get("commit") != server_commit or client_mino.get(
        "peer_commit"
    ) != server_commit:
        errors.append("client Mino payload did not corroborate server commit")
    if server_peer.get("identity") != client_local.get(
        "identity"
    ) or server_mino.get("peer_machine_identity") != client_local.get("identity"):
        errors.append("server Mino payload did not corroborate client machine identity")
    if client_peer.get("identity") != server_local.get(
        "identity"
    ) or client_mino.get("peer_machine_identity") != server_local.get("identity"):
        errors.append("client Mino payload did not corroborate server machine identity")
    if server_local.get("identity") == client_local.get("identity"):
        errors.append("server and client machine identities are equal")
    server_binary_hash = server_binary.get("sha256")
    client_binary_hash = client_binary.get("sha256")
    binary_identical = (
        isinstance(server_binary_hash, str)
        and DIGEST_RE.fullmatch(server_binary_hash) is not None
        and server_binary_hash == client_binary_hash
    )
    if not binary_identical:
        errors.append("server and client binary hashes are missing or different")
    hosted_server_hash = builds.get("server", {}).get("binary_sha256")
    hosted_client_hash = builds.get("client", {}).get("binary_sha256")
    hosted_binary_bound = (
        binary_identical
        and server_binary_hash == hosted_server_hash
        and client_binary_hash == hosted_client_hash
    )
    if not hosted_binary_bound:
        errors.append(
            "role binary hashes do not match both hosted build provenances"
        )
    if server_peer.get("advertised_address") != client_local.get("advertised_address"):
        errors.append("server did not receive the client advertised address over Mino")
    if client_peer.get("advertised_address") != server_local.get("advertised_address"):
        errors.append("client did not receive the server advertised address over Mino")
    if server_mino.get("local_schema_digest") != client_mino.get("peer_schema_digest"):
        errors.append("server local schema is not corroborated by client")
    if client_mino.get("local_schema_digest") != server_mino.get("peer_schema_digest"):
        errors.append("client local schema is not corroborated by server")

    for role, result in (("server", server_mino), ("client", client_mino)):
        if result.get("schema_version") != MINO_RESULT_SCHEMA_VERSION:
            errors.append(f"{role} Mino result schema version mismatch")
        for field in REQUIRED_MINO_TRUE_FIELDS:
            if result.get(field) is not True:
                errors.append(f"{role} Mino evidence {field} is not true")
        for field, minimum in (
            ("completed_handshakes", 3),
            ("reconnects", 2),
            ("disconnects", 2),
            ("accepted_acks", 3),
            ("duplicate_checks", 1),
            ("descriptor_authentications", 1),
            ("descriptor_persistences", 1),
            ("descriptor_artifact_bytes", 1),
            ("accepted_connections" if role == "server" else "connection_attempts", 3),
        ):
            value = result.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                errors.append(
                    f"{role} Mino counter {field} is below required minimum {minimum}"
                )
        local_schema_digest = result.get("local_schema_digest")
        peer_schema_digest = result.get("peer_schema_digest")
        if (
            not isinstance(local_schema_digest, str)
            or DIGEST_RE.fullmatch(local_schema_digest) is None
            or not isinstance(peer_schema_digest, str)
            or DIGEST_RE.fullmatch(peer_schema_digest) is None
            or hmac.compare_digest(local_schema_digest, peer_schema_digest)
        ):
            errors.append(f"{role} schema digests are invalid or identical")
        for prefix in ("local", "remote"):
            initial = result.get(f"initial_{prefix}_session_epoch")
            final_epoch = result.get(f"{prefix}_session_epoch")
            if (
                not isinstance(initial, int)
                or isinstance(initial, bool)
                or initial <= 0
                or not isinstance(final_epoch, int)
                or isinstance(final_epoch, bool)
                or final_epoch <= 0
                or initial == final_epoch
            ):
                errors.append(f"{role} {prefix} session epoch did not change")

    server_topic_routes: dict[tuple[int, str], int] = {}
    client_topic_routes: dict[tuple[int, str], int] = {}
    server_network_scaling: dict[tuple[str, int], Mapping[str, Any]] = {}
    client_network_scaling: dict[tuple[str, int], Mapping[str, Any]] = {}
    server_latency_samples: dict[int, Mapping[str, Any]] = {}
    client_latency_samples: dict[int, Mapping[str, Any]] = {}
    topic_route_counterparts_match = False
    network_workload_dimensions_match = False
    network_counterpart_messages_match = False
    latency_workload_dimensions_match = False
    if validated_results == {"server", "client"}:
        server_topic_routes = _validate_topic_routes(
            server_mino["topic_routes"], role="server"
        )
        client_topic_routes = _validate_topic_routes(
            client_mino["topic_routes"], role="client"
        )
        topic_route_counterparts_match = all(
            messages
            == client_topic_routes[
                (topic_id, "received" if direction == "sent" else "sent")
            ]
            for (topic_id, direction), messages in server_topic_routes.items()
        )
        if not topic_route_counterparts_match:
            errors.append("server and client topic-route counterpart messages differ")

        server_network_scaling = _validate_network_scaling(
            server_mino["network_scaling"]
        )
        client_network_scaling = _validate_network_scaling(
            client_mino["network_scaling"]
        )
        network_workload_dimensions_match = all(
            server_network_scaling[key]["messages_per_topic"]
            == client_network_scaling[key]["messages_per_topic"]
            and server_network_scaling[key]["application_payload_bytes"]
            == client_network_scaling[key]["application_payload_bytes"]
            for key in server_network_scaling
        )
        if not network_workload_dimensions_match:
            errors.append(
                "server and client network-scaling workload dimensions differ"
            )
        network_counterpart_messages_match = all(
            server_network_scaling[key]["messages_sent"]
            == client_network_scaling[key]["messages_received"]
            and client_network_scaling[key]["messages_sent"]
            == server_network_scaling[key]["messages_received"]
            for key in server_network_scaling
        )
        if not network_counterpart_messages_match:
            errors.append(
                "server and client network-scaling counterpart messages differ"
            )

        server_latency_samples = _validate_latency_samples(
            server_mino["latency_samples"]
        )
        client_latency_samples = _validate_latency_samples(
            client_mino["latency_samples"]
        )
        latency_workload_dimensions_match = all(
            (
                server_latency_samples[topic_count]["topic_count"],
                server_latency_samples[topic_count]["messages_per_topic"],
                server_latency_samples[topic_count]["sample_count"],
            )
            == (
                client_latency_samples[topic_count]["topic_count"],
                client_latency_samples[topic_count]["messages_per_topic"],
                client_latency_samples[topic_count]["sample_count"],
            )
            for topic_count in LATENCY_TOPIC_COUNTS
        )
        if not latency_workload_dimensions_match:
            errors.append("server and client latency workload dimensions differ")

    def scaling_constant(
        records: Mapping[tuple[str, int], Mapping[str, Any]],
        mode: str,
        field: str,
    ) -> int | None:
        values = {
            record[field]
            for (record_mode, _), record in records.items()
            if record_mode == mode
        }
        return next(iter(values)) if len(values) == 1 else None

    def scaling_samples(
        records: Mapping[tuple[str, int], Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        for mode in NETWORK_SCALING_MODES:
            for topic_count in NETWORK_SCALING_TOPIC_COUNTS:
                record = records.get((mode, topic_count))
                if record is None:
                    continue
                elapsed_ms = record["elapsed_ms"]
                sent_payload_bytes = record["application_payload_bytes"]
                tcp_framed_bytes = record["tcp_framed_bytes"]
                sample = dict(record)
                sample.update(
                    {
                        "payload_bytes_per_message": sent_payload_bytes
                        // record["messages_sent"],
                        "tcp_framed_to_sent_payload_ratio": round(
                            tcp_framed_bytes / sent_payload_bytes, 6
                        ),
                        "messages_per_second": round(
                            record["messages_sent"] * 1000 / elapsed_ms, 3
                        ),
                        "sent_payload_bytes_per_second": round(
                            sent_payload_bytes * 1000 / elapsed_ms, 3
                        ),
                        "tcp_framed_bytes_per_second": round(
                            tcp_framed_bytes * 1000 / elapsed_ms, 3
                        ),
                    }
                )
                samples.append(sample)
        return samples

    def latency_samples_with_increases(
        records: Mapping[int, Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        baseline = records.get(1)
        if baseline is None:
            return []
        samples: list[dict[str, Any]] = []
        for topic_count in LATENCY_TOPIC_COUNTS:
            record = records.get(topic_count)
            if record is None:
                continue
            sample = dict(record)
            for field in LATENCY_RTT_FIELDS:
                metric = field.removesuffix("_us")
                increase = record[field] - baseline[field]
                sample[f"{metric}_increase_us"] = increase
                sample[f"{metric}_increase_percent"] = round(
                    increase * 100 / baseline[field], 3
                )
            samples.append(sample)
        return samples

    topic_routes_summary = {
        "expected_records_per_host": 4,
        "server_records": len(server_topic_routes),
        "client_records": len(client_topic_routes),
        "topic_ids": [1001, 1002, 2001, 2002],
        "counterpart_messages_match": topic_route_counterparts_match,
        "messages_sent": {
            "server": sum(
                messages
                for (_, direction), messages in server_topic_routes.items()
                if direction == "sent"
            ),
            "client": sum(
                messages
                for (_, direction), messages in client_topic_routes.items()
                if direction == "sent"
            ),
        },
        "messages_received": {
            "server": sum(
                messages
                for (_, direction), messages in server_topic_routes.items()
                if direction == "received"
            ),
            "client": sum(
                messages
                for (_, direction), messages in client_topic_routes.items()
                if direction == "received"
            ),
        },
    }
    latency_summary = {
        "semantics": {
            "measurement": "Bridge dispatch-to-ingress echo RTT",
            "scope": (
                "includes probe payload construction and validation, dispatcher, "
                "Bridge pipeline, TCP, and remote echo dispatch; excludes local Bus "
                "subscriber scheduling"
            ),
            "clock": "initiator local steady clock",
            "cross_host_clock_synchronization": False,
            "single_message_rtt_us": "first completed echo RTT in the phase",
            "concurrency": "at most one outstanding request per topic",
            "distribution": "64 completed RTT samples per topic",
            "path": [
                "local DispatchTargets",
                "Bridge pipeline",
                "TCP",
                "remote ingress",
                "remote DispatchTargets",
                "return pipeline",
                "local ingress",
            ],
        },
        "topic_counts": list(LATENCY_TOPIC_COUNTS),
        "messages_per_topic": LATENCY_MESSAGES_PER_TOPIC,
        "workload_dimensions_match": latency_workload_dimensions_match,
        "samples": {
            "server": latency_samples_with_increases(server_latency_samples),
            "client": latency_samples_with_increases(client_latency_samples),
        },
    }
    network_scaling_summary = {
        "expected_records_per_host": 12,
        "server_records": len(server_network_scaling),
        "client_records": len(client_network_scaling),
        "modes": list(NETWORK_SCALING_MODES),
        "topic_counts": list(NETWORK_SCALING_TOPIC_COUNTS),
        "workload_dimensions_match": network_workload_dimensions_match,
        "counterpart_messages_match": network_counterpart_messages_match,
        "byte_accounting": {
            "mino_frame_body_bytes": (
                "aggregate local BridgePipeline inbound and outbound frame bodies"
            ),
            "tcp_prefix_bytes": (
                "four-byte TCP length prefix per observed inbound or outbound frame"
            ),
            "tcp_framed_bytes": "Mino frame bodies plus TCP length prefixes",
            "excludes": [
                "IP headers",
                "TCP headers and retransmission overhead below TcpDriver",
                "session discovery and health traffic outside BridgePipeline",
                "TcpDriver heartbeat traffic",
            ],
            "host_totals_must_not_be_added": (
                "the same transfer is observed once by each physical host"
            ),
        },
        "fixed_total_messages": {
            "server": scaling_constant(
                server_network_scaling, "fixed_total", "messages_sent"
            ),
            "client": scaling_constant(
                client_network_scaling, "fixed_total", "messages_sent"
            ),
        },
        "fixed_per_topic_messages": {
            "server": scaling_constant(
                server_network_scaling, "fixed_per_topic", "messages_per_topic"
            ),
            "client": scaling_constant(
                client_network_scaling, "fixed_per_topic", "messages_per_topic"
            ),
        },
        "samples": {
            "server": scaling_samples(server_network_scaling),
            "client": scaling_samples(client_network_scaling),
        },
    }

    logs: dict[str, Any] = {}
    role_hashes: dict[str, Any] = {}
    mino_results: dict[str, Any] = {}
    persisted_schemas: dict[str, Any] = {}
    for role, path in (
        ("server", server_manifest_path),
        ("client", client_manifest_path),
    ):
        if path.is_file():
            role_hashes[role] = {
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        manifest = roles.get(role, {})
        log_record = _json_object(manifest.get("log"))
        log_name = log_record.get("path")
        if not isinstance(log_name, str) or Path(log_name).name != log_name:
            errors.append(f"{role} log path is invalid")
            continue
        log_path = path.parent / log_name
        if not log_path.is_file():
            errors.append(f"{role} log is missing")
            continue
        actual_hash = sha256_file(log_path)
        if actual_hash != log_record.get("sha256"):
            errors.append(f"{role} log hash mismatch")
        if log_path.stat().st_size != log_record.get("size_bytes"):
            errors.append(f"{role} log size mismatch")
        logs[role] = {
            "path": str(log_path),
            "sha256": actual_hash,
            "size_bytes": log_path.stat().st_size,
        }

        result_record = _json_object(manifest.get("mino_result_artifact"))
        result_name = result_record.get("path")
        if not isinstance(result_name, str) or Path(result_name).name != result_name:
            errors.append(f"{role} Mino result artifact path is invalid")
        else:
            role_result_path = path.parent / result_name
            if not role_result_path.is_file():
                errors.append(f"{role} Mino result artifact is missing")
            else:
                actual_result_hash = sha256_file(role_result_path)
                if actual_result_hash != result_record.get("sha256"):
                    errors.append(f"{role} Mino result artifact hash mismatch")
                if role_result_path.stat().st_size != result_record.get("size_bytes"):
                    errors.append(f"{role} Mino result artifact size mismatch")
                try:
                    persisted_result = _load_json_object(
                        role_result_path, f"{role} Mino result artifact"
                    )
                except ProtocolError as error:
                    errors.append(str(error))
                else:
                    if persisted_result != _json_object(manifest.get("mino")):
                        errors.append(
                            f"{role} Mino result artifact differs from role manifest"
                        )
                mino_results[role] = {
                    "path": str(role_result_path),
                    "sha256": actual_result_hash,
                    "size_bytes": role_result_path.stat().st_size,
                }

        schema_records = manifest.get("schema_artifacts")
        result = _json_object(manifest.get("mino"))
        expected_digest = result.get("peer_schema_digest")
        expected_relative_path = (
            f"schema-store/schemas/{expected_digest}.schema"
            if isinstance(expected_digest, str)
            else None
        )
        if not isinstance(schema_records, list) or len(schema_records) != 1:
            errors.append(
                f"{role} must record exactly one persisted peer schema artifact"
            )
            continue
        checked_schemas: list[dict[str, Any]] = []
        for index, raw_record in enumerate(schema_records):
            record = _json_object(raw_record)
            relative_name = record.get("path")
            if not isinstance(relative_name, str):
                errors.append(f"{role} schema artifact {index} path is invalid")
                continue
            relative_path = Path(relative_name)
            if (
                relative_path.is_absolute()
                or ".." in relative_path.parts
                or relative_path.suffix != ".schema"
                or relative_path.as_posix() != expected_relative_path
                or record.get("canonical_digest") != expected_digest
                or record.get("identity_verified") is not True
                or record.get("bytes_verified") is not True
                or result.get("persisted_schema_digest") != expected_digest
            ):
                errors.append(
                    f"{role} schema artifact {index} identity, digest, or path is invalid"
                )
                continue
            artifact_path = path.parent / relative_path
            if not artifact_path.is_file() or artifact_path.stat().st_size <= 0:
                errors.append(f"{role} schema artifact {index} is missing or empty")
                continue
            actual_artifact_hash = sha256_file(artifact_path)
            if actual_artifact_hash != record.get("sha256"):
                errors.append(f"{role} schema artifact {index} hash mismatch")
            if artifact_path.stat().st_size != record.get("size_bytes"):
                errors.append(f"{role} schema artifact {index} size mismatch")
            checked_schemas.append(
                {
                    "path": str(artifact_path),
                    "canonical_digest": expected_digest,
                    "identity_verified": True,
                    "bytes_verified": True,
                    "sha256": actual_artifact_hash,
                    "size_bytes": artifact_path.stat().st_size,
                }
            )
        if checked_schemas:
            persisted_schemas[role] = checked_schemas

    final = {
        "schema_version": SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "outcome": "passed" if not errors else "failed",
        "generated_at": utc_now(),
        "commits": {"server": server_commit, "client": client_commit},
        "run": {
            "expected_id": expected_run_id,
            "expected_attempt": expected_run_attempt,
            "server_id": _json_object(server.get("github")).get("github_run_id"),
            "server_attempt": _json_object(server.get("github")).get("github_run_attempt"),
            "client_id": _json_object(client.get("github")).get("github_run_id"),
            "client_attempt": _json_object(client.get("github")).get("github_run_attempt"),
        },
        "addresses": {
            "server_advertised": server_local.get("advertised_address"),
            "client_advertised": client_local.get("advertised_address"),
            "client_configured_server": client_peer.get("configured_address"),
        },
        "host_identities": {
            "server": server_local.get("identity"),
            "client": client_local.get("identity"),
            "distinct": server_local.get("identity") != client_local.get("identity"),
            "derivation": "HMAC-SHA256(run-token, machine-id); raw values are not recorded",
        },
        "binary": {
            "server_sha256": server_binary_hash,
            "client_sha256": client_binary_hash,
            "hosted_server_sha256": hosted_server_hash,
            "hosted_client_sha256": hosted_client_hash,
            "identical": binary_identical,
            "bound_to_hosted_builds": hosted_binary_bound,
            "hosted_builds": builds,
        },
        "mino_data_path": {
            "transport": "TcpDriver",
            "connection": "BridgeConnectionManager",
            "session_discovery": server_mino.get("session_discovery") is True
            and client_mino.get("session_discovery") is True,
            "pipeline": "BridgePipeline",
            "reliability": "ReliableOrdered",
            "server_remote_acknowledged": server_mino.get("remote_acknowledged")
            is True,
            "client_remote_acknowledged": client_mino.get("remote_acknowledged")
            is True,
            "bidirectional_ack": server_mino.get("bidirectional_ack") is True
            and client_mino.get("bidirectional_ack") is True,
            "topic_routes": topic_routes_summary,
            "network_scaling": network_scaling_summary,
            "latency": latency_summary,
        },
        "reconnect": {
            "forced_disconnect": server_mino.get("forced_disconnect") is True
            and client_mino.get("forced_disconnect") is True,
            "automatic": server_mino.get("automatic_reconnect") is True
            and client_mino.get("automatic_reconnect") is True,
            "session_epoch_changed": server_mino.get("session_epoch_changed") is True
            and client_mino.get("session_epoch_changed") is True,
            "pending_reliable_recovered": server_mino.get("pending_reliable_recovered") is True
            and client_mino.get("pending_reliable_recovered") is True,
            "pending_reliable_retransmitted": server_mino.get("pending_reliable_retransmitted") is True
            and client_mino.get("pending_reliable_retransmitted") is True,
            "reliable_replay_pending_observed": server_mino.get("reliable_replay_pending_observed") is True
            and client_mino.get("reliable_replay_pending_observed") is True,
            "resume_mode": "pending-retransmit-then-explicit-dedup-replay",
            "duplicate_suppressed": server_mino.get("duplicate_suppressed") is True
            and client_mino.get("duplicate_suppressed") is True,
        },
        "schema": {
            "identity_nonempty": server_mino.get("schema_identity_nonempty") is True
            and client_mino.get("schema_identity_nonempty") is True,
            "artifact_nonempty": server_mino.get("descriptor_artifact_nonempty") is True
            and client_mino.get("descriptor_artifact_nonempty") is True,
            "announcement": server_mino.get("schema_announcement") is True
            and client_mino.get("schema_announcement") is True,
            "request": server_mino.get("schema_request") is True
            and client_mino.get("schema_request") is True,
            "persisted": server_mino.get("schema_persisted") is True
            and client_mino.get("schema_persisted") is True,
            "persisted_identity_verified": server_mino.get(
                "persisted_schema_identity_verified"
            )
            is True
            and client_mino.get("persisted_schema_identity_verified") is True,
            "persisted_bytes_verified": server_mino.get(
                "persisted_schema_bytes_verified"
            )
            is True
            and client_mino.get("persisted_schema_bytes_verified") is True,
            "server_digest": server_mino.get("local_schema_digest"),
            "client_digest": client_mino.get("local_schema_digest"),
        },
        "logs": logs,
        "mino_results": mino_results,
        "persisted_schema_artifacts": persisted_schemas,
        "role_manifests": role_hashes,
        "errors": errors,
        "github": github_provenance(os.environ),
    }
    write_json_atomic(output_path, final)
    print(f"final_manifest={output_path}", flush=True)
    return 0 if not errors else 1


def self_test() -> None:
    token = b"self-test-token-that-is-at-least-thirty-two-bytes"
    github_sha = os.environ.get("GITHUB_SHA", "")
    commit = github_sha.lower() if COMMIT_RE.fullmatch(github_sha) else "a" * 40
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        fake = root / "fake-mino-probe.py"
        fake.write_text(
            """#!/usr/bin/env python3
import json
import sys
import time
from pathlib import Path
args = dict(arg[2:].split('=', 1) for arg in sys.argv[1:])
if args['address'] == 'slow.invalid':
    time.sleep(30)
role = args['role']
peer_identity = 'b' * 64 if role == 'server' else 'a' * 64
peer_address = '10.0.0.2:43191' if role == 'server' else '10.0.0.1:43191'
route_directions = (
    ((1001, 'sent'), (1002, 'sent'), (2001, 'received'), (2002, 'received'))
    if role == 'server'
    else ((1001, 'received'), (1002, 'received'), (2001, 'sent'), (2002, 'sent'))
)
topic_routes = [
    {
        'topic_id': topic_id,
        'direction': direction,
        'messages': 64,
        'ordered': True,
        'payload_verified': True,
        'cross_topic_leakage': False,
    }
    for topic_id, direction in route_directions
]
network_scaling = []
role_offset = 7 if role == 'server' else 11
for mode in ('fixed_total', 'fixed_per_topic'):
    for topic_count in (1, 2, 4, 8, 16, 32):
        messages_per_topic = 384 // topic_count if mode == 'fixed_total' else 32
        messages = topic_count * messages_per_topic
        inbound_frames = messages + 1
        outbound_frames = messages + 2
        prefix_bytes = 4 * (inbound_frames + outbound_frames)
        frame_body_bytes = messages * 24 + role_offset
        network_scaling.append({
            'mode': mode,
            'topic_count': topic_count,
            'messages_per_topic': messages_per_topic,
            'messages_sent': messages,
            'messages_received': messages,
            'application_payload_bytes': messages * 256,
            'mino_frame_body_bytes': frame_body_bytes,
            'pipeline_inbound_frames': inbound_frames,
            'pipeline_outbound_frames': outbound_frames,
            'tcp_prefix_bytes': prefix_bytes,
            'tcp_framed_bytes': frame_body_bytes + prefix_bytes,
            'elapsed_ms': topic_count + role_offset,
            'accepted_acks': messages + 1,
            'retransmissions': 0,
            'duplicate_suppressed': 0,
            'cross_topic_leakage': False,
        })
latency_samples = []
latency_offset = 100 if role == 'server' else 200
for topic_count in (1, 2, 4, 8, 16, 32):
    latency_samples.append({
        'topic_count': topic_count,
        'messages_per_topic': 64,
        'sample_count': topic_count * 64,
        'single_message_rtt_us': latency_offset + topic_count,
        'p50_rtt_us': latency_offset + topic_count * 2,
        'p95_rtt_us': latency_offset + topic_count * 3,
        'p99_rtt_us': latency_offset + topic_count * 4,
        'max_rtt_us': latency_offset + topic_count * 5,
    })
tcp_lane_count = int(args.get('tcp-lane-count', '1'))
lane_connections = []
for lane_index in range(tcp_lane_count):
    lane_connections.append({
        'lane_index': lane_index,
        'active': True,
        'local_session_epoch': 33 + lane_index,
        'remote_session_epoch': 44 + lane_index,
        'connection_attempts': 1 if role == 'client' else 0,
        'accepted_connections': 1 if role == 'server' else 0,
        'completed_handshakes': 1,
        'reconnects': 2 if lane_index == 0 else 0,
        'disconnects': 2 if lane_index == 0 else 0,
    })
result = {
    'schema_version': 6,
    'protocol': 'mino-two-host-mino-v2',
    'role': role,
    'outcome': 'passed',
    'commit': args['commit'],
    'machine_identity': args['machine-identity'],
    'peer_commit': args['commit'],
    'peer_machine_identity': peer_identity,
    'local_address': args['advertise-address'] + ':' + args['port'],
    'peer_address': peer_address,
    'session_discovery': True,
    'bridge_active': True,
    'reliable_sent': True,
    'reliable_received': True,
    'remote_acknowledged': True,
    'schema_identity_nonempty': True,
    'descriptor_artifact_nonempty': True,
    'schema_announcement': True,
    'schema_request': True,
    'schema_persisted': True,
    'persisted_schema_identity_verified': True,
    'persisted_schema_bytes_verified': True,
    'forced_disconnect': True,
    'automatic_reconnect': True,
    'session_epoch_changed': True,
    'pending_reliable_before_disconnect': True,
    'pending_reliable_recovered': True,
    'pending_reliable_retransmitted': True,
    'reliable_replay_sent': True,
    'reliable_replay_pending_observed': True,
    'dedup_state_preserved': True,
    'duplicate_suppressed': True,
    'bidirectional_ack': True,
    'tcp_lane_count': tcp_lane_count,
    'active_lane_connections': tcp_lane_count,
    'exercised_lane_count': tcp_lane_count,
    'lane_connections': lane_connections,
    'topic_routes': topic_routes,
    'network_scaling': network_scaling,
    'latency_samples': latency_samples,
    'local_schema_digest': ('c' * 64 if role == 'server' else 'd' * 64),
    'peer_schema_digest': ('d' * 64 if role == 'server' else 'c' * 64),
    'persisted_schema_digest': ('d' * 64 if role == 'server' else 'c' * 64),
    'initial_local_session_epoch': 11,
    'initial_remote_session_epoch': 22,
    'local_session_epoch': 33,
    'remote_session_epoch': 44,
    'connection_attempts': 3 if role == 'client' else 0,
    'accepted_connections': 3 if role == 'server' else 0,
    'completed_handshakes': 3,
    'reconnects': 2,
    'disconnects': 2,
    'accepted_acks': 3,
    'duplicate_checks': 1,
    'descriptor_authentications': 1,
    'descriptor_persistences': 1,
    'descriptor_artifact_bytes': 512,
    'elapsed_ms': 1,
    'error': '',
}
path = Path(args['output'])
path.parent.mkdir(parents=True, exist_ok=True)
schema_path = path.parent / 'schema-store' / 'schemas' / (result['peer_schema_digest'] + '.schema')
schema_path.parent.mkdir(parents=True, exist_ok=True)
schema_path.write_bytes(b'MINODSC2-self-test-artifact')
path.write_text(json.dumps(result), encoding='utf-8')
print('fake mino probe completed')
""",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        build_roots: dict[str, Path] = {}
        for role in ("server", "client"):
            build_root = root / "build" / role
            build_root.mkdir(parents=True)
            build_binary = build_root / "mino_two_host_probe"
            build_binary.write_bytes(fake.read_bytes())
            build_binary.chmod(0o755)
            build_hash = sha256_file(build_binary)
            (build_root / "mino_two_host_probe.sha256").write_text(
                f"{build_hash}  mino_two_host_probe\n", encoding="ascii"
            )
            (build_root / "commit.txt").write_text(commit + "\n", encoding="ascii")
            write_json_atomic(
                build_root / "provenance.json",
                {
                    "commit": commit,
                    "run_id": "self-test-run",
                    "run_attempt": "1",
                    "workflow": "self-test",
                },
            )
            build_roots[role] = build_root
        server_manifest = root / "server" / "manifest.json"
        client_manifest = root / "client" / "manifest.json"
        assert run_role(
            role="server",
            binary=fake,
            address="0.0.0.0",
            advertise_address="10.0.0.1",
            port=43191,
            timeout_seconds=2,
            token=token,
            commit=commit,
            manifest_path=server_manifest,
            log_path=server_manifest.parent / "server.log",
            allow_short_timeout=True,
            identity_override="a" * 64,
        ) == 0
        assert run_role(
            role="client",
            binary=fake,
            address="10.0.0.1",
            advertise_address="10.0.0.2",
            port=43191,
            timeout_seconds=2,
            token=token,
            commit=commit,
            manifest_path=client_manifest,
            log_path=client_manifest.parent / "client.log",
            allow_short_timeout=True,
            identity_override="b" * 64,
        ) == 0
        final_path = root / "final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=final_path,
            expected_commit=commit,
        ) == 0
        final = _load_json_object(final_path, "self-test final manifest")
        assert final["schema_version"] == SCHEMA_VERSION
        assert final["outcome"] == "passed"
        assert final["binary"]["identical"]
        assert final["binary"]["bound_to_hosted_builds"]
        assert final["mino_data_path"]["server_remote_acknowledged"]
        assert final["mino_data_path"]["client_remote_acknowledged"]
        topic_summary = final["mino_data_path"]["topic_routes"]
        assert topic_summary["server_records"] == 4
        assert topic_summary["client_records"] == 4
        assert topic_summary["counterpart_messages_match"]
        scaling_summary = final["mino_data_path"]["network_scaling"]
        assert scaling_summary["server_records"] == 12
        assert scaling_summary["client_records"] == 12
        assert scaling_summary["workload_dimensions_match"]
        assert scaling_summary["counterpart_messages_match"]
        assert scaling_summary["fixed_total_messages"] == {
            "server": 384,
            "client": 384,
        }
        assert scaling_summary["fixed_per_topic_messages"] == {
            "server": 32,
            "client": 32,
        }
        assert len(scaling_summary["samples"]["server"]) == 12
        assert len(scaling_summary["samples"]["client"]) == 12
        assert scaling_summary["samples"]["server"][0][
            "payload_bytes_per_message"
        ] == 256
        latency_summary = final["mino_data_path"]["latency"]
        assert latency_summary["semantics"]["measurement"] == (
            "Bridge dispatch-to-ingress echo RTT"
        )
        assert latency_summary["semantics"]["clock"] == "initiator local steady clock"
        assert latency_summary["semantics"]["cross_host_clock_synchronization"] is False
        assert latency_summary["topic_counts"] == [1, 2, 4, 8, 16, 32]
        assert latency_summary["messages_per_topic"] == 64
        assert latency_summary["workload_dimensions_match"]
        server_latency = latency_summary["samples"]["server"]
        client_latency = latency_summary["samples"]["client"]
        assert [sample["topic_count"] for sample in server_latency] == [
            1,
            2,
            4,
            8,
            16,
            32,
        ]
        assert [sample["topic_count"] for sample in client_latency] == [
            1,
            2,
            4,
            8,
            16,
            32,
        ]
        assert server_latency[0]["single_message_rtt_us"] != client_latency[0][
            "single_message_rtt_us"
        ]
        for metric in ("single_message_rtt", "p50_rtt", "p95_rtt", "p99_rtt", "max_rtt"):
            assert server_latency[0][f"{metric}_increase_us"] == 0
            assert server_latency[0][f"{metric}_increase_percent"] == 0
            assert f"{metric}_increase_us" in client_latency[0]
            assert f"{metric}_increase_percent" in client_latency[0]
        assert server_latency[1]["single_message_rtt_increase_us"] == 1
        assert server_latency[1]["single_message_rtt_increase_percent"] == 0.99
        assert server_latency[1]["p99_rtt_increase_us"] == 4
        assert server_latency[1]["p99_rtt_increase_percent"] == 3.846
        assert final["reconnect"]["automatic"]
        assert final["reconnect"]["duplicate_suppressed"]
        assert final["schema"]["request"]
        assert final["schema"]["persisted"]

        valid_server_manifest = _load_json_object(
            server_manifest, "server validation self-test manifest"
        )
        valid_server_result = valid_server_manifest["mino"]

        def expect_invalid_result(
            candidate: Mapping[str, Any], expected_error: str
        ) -> None:
            try:
                _validate_mino_result(
                    candidate,
                    role="server",
                    commit=commit,
                    local_identity="a" * 64,
                    tcp_lane_count=1,
                )
            except ProtocolError as error:
                assert expected_error in str(error), str(error)
            else:
                raise AssertionError("tampered Mino result was accepted")

        invalid = json.loads(json.dumps(valid_server_result))
        invalid["schema_version"] = 4
        expect_invalid_result(invalid, "schema_version")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["topic_routes"][0]["unexpected"] = True
        expect_invalid_result(invalid, "exactly the required keys")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["topic_routes"][0]["messages"] = True
        expect_invalid_result(invalid, "at least 64")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["topic_routes"][0]["messages"] = 63
        expect_invalid_result(invalid, "at least 64")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["topic_routes"][1] = dict(invalid["topic_routes"][0])
        expect_invalid_result(invalid, "duplicates")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["topic_routes"][0]["cross_topic_leakage"] = True
        expect_invalid_result(invalid, "cross-topic leakage")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["unexpected"] = 1
        expect_invalid_result(invalid, "exactly the required keys")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["elapsed_ms"] = True
        expect_invalid_result(invalid, "positive integer")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["retransmissions"] = -1
        expect_invalid_result(invalid, "nonnegative integer")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["cross_topic_leakage"] = True
        expect_invalid_result(invalid, "cross-topic leakage")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["tcp_prefix_bytes"] += 4
        expect_invalid_result(invalid, "TCP prefix bytes")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["accepted_acks"] = 1
        expect_invalid_result(invalid, "accepted ACK count")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["network_scaling"][0]["application_payload_bytes"] += 1
        expect_invalid_result(invalid, "256 bytes per message")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid_record = invalid["network_scaling"][0]
        invalid_record["messages_per_topic"] *= 2
        invalid_record["messages_sent"] *= 2
        invalid_record["messages_received"] *= 2
        invalid_record["accepted_acks"] = invalid_record["messages_sent"]
        expect_invalid_result(invalid, "required workload")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["unexpected"] = 1
        expect_invalid_result(invalid, "exactly the required keys")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"].pop()
        expect_invalid_result(invalid, "array of six records")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][1] = dict(invalid["latency_samples"][0])
        expect_invalid_result(invalid, "duplicates topic_count")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["p50_rtt_us"] = True
        expect_invalid_result(invalid, "positive integer")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["max_rtt_us"] = 0
        expect_invalid_result(invalid, "positive integer")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["sample_count"] += 1
        expect_invalid_result(invalid, "sample_count is inconsistent")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["messages_per_topic"] = 63
        expect_invalid_result(invalid, "messages_per_topic is not 64")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["p50_rtt_us"] = (
            invalid["latency_samples"][0]["p95_rtt_us"] + 1
        )
        expect_invalid_result(invalid, "percentile order is invalid")
        invalid = json.loads(json.dumps(valid_server_result))
        invalid["latency_samples"][0]["single_message_rtt_us"] = (
            invalid["latency_samples"][0]["max_rtt_us"] + 1
        )
        expect_invalid_result(invalid, "single-message RTT exceeds max RTT")

        original_client_manifest = client_manifest.read_bytes()
        client_result_path = client_manifest.parent / "mino-result.json"
        original_client_result = client_result_path.read_bytes()

        def persist_client_result(manifest: dict[str, Any]) -> None:
            write_json_atomic(client_result_path, manifest["mino"])
            manifest["mino_result_artifact"]["sha256"] = sha256_file(
                client_result_path
            )
            manifest["mino_result_artifact"]["size_bytes"] = (
                client_result_path.stat().st_size
            )
            write_json_atomic(client_manifest, manifest)

        route_tampered = _load_json_object(
            client_manifest, "client route-tampered self-test manifest"
        )
        route_tampered["mino"]["topic_routes"][0]["messages"] += 1
        persist_client_result(route_tampered)
        route_tampered_final = root / "route-tampered-final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=route_tampered_final,
            expected_commit=commit,
        ) == 1
        route_failure = _load_json_object(
            route_tampered_final, "route-tampered self-test final manifest"
        )
        assert any(
            "correctness message count" in error
            for error in route_failure["errors"]
        )
        client_manifest.write_bytes(original_client_manifest)
        client_result_path.write_bytes(original_client_result)

        scaling_tampered = _load_json_object(
            client_manifest, "client scaling-tampered self-test manifest"
        )
        for record in scaling_tampered["mino"]["network_scaling"]:
            if record["mode"] == "fixed_per_topic":
                record["messages_per_topic"] = 4
                messages = record["topic_count"] * record["messages_per_topic"]
                record["messages_sent"] = messages
                record["messages_received"] = messages
                record["application_payload_bytes"] = messages * 256
                record["accepted_acks"] = messages
        persist_client_result(scaling_tampered)
        scaling_tampered_final = root / "scaling-tampered-final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=scaling_tampered_final,
            expected_commit=commit,
        ) == 1
        scaling_failure = _load_json_object(
            scaling_tampered_final, "scaling-tampered self-test final manifest"
        )
        assert any(
            "required workload" in error
            for error in scaling_failure["errors"]
        )
        client_manifest.write_bytes(original_client_manifest)
        client_result_path.write_bytes(original_client_result)

        latency_tampered = _load_json_object(
            client_manifest, "client latency-tampered self-test manifest"
        )
        latency_tampered["mino"]["latency_samples"][0][
            "messages_per_topic"
        ] = 63
        persist_client_result(latency_tampered)
        latency_tampered_final = root / "latency-tampered-final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=latency_tampered_final,
            expected_commit=commit,
        ) == 1
        latency_failure = _load_json_object(
            latency_tampered_final, "latency-tampered self-test final manifest"
        )
        assert latency_failure["outcome"] == "failed"
        assert not latency_failure["mino_data_path"]["latency"][
            "workload_dimensions_match"
        ]
        assert any(
            "latency" in error and "messages_per_topic is not 64" in error
            for error in latency_failure["errors"]
        )
        client_manifest.write_bytes(original_client_manifest)
        client_result_path.write_bytes(original_client_result)

        nested_tampered = _load_json_object(
            client_manifest, "client nested-result self-test manifest"
        )
        nested_tampered["mino"]["protocol"] = "tampered-protocol"
        write_json_atomic(client_result_path, nested_tampered["mino"])
        nested_tampered["mino_result_artifact"]["sha256"] = sha256_file(
            client_result_path
        )
        nested_tampered["mino_result_artifact"]["size_bytes"] = (
            client_result_path.stat().st_size
        )
        write_json_atomic(client_manifest, nested_tampered)
        nested_tampered_final = root / "nested-tampered-final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=nested_tampered_final,
            expected_commit=commit,
        ) == 1
        nested_failure = _load_json_object(
            nested_tampered_final, "nested-tampered self-test final manifest"
        )
        assert any("nested Mino result" in error for error in nested_failure["errors"])
        client_manifest.write_bytes(original_client_manifest)
        client_result_path.write_bytes(original_client_result)

        tampered = _load_json_object(client_manifest, "client self-test manifest")
        tampered["mino"]["schema_persisted"] = False
        write_json_atomic(client_manifest, tampered)
        tampered_final = root / "tampered-final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=tampered_final,
            expected_commit=commit,
        ) == 1
        assert _load_json_object(
            tampered_final, "tampered self-test final manifest"
        )["outcome"] == "failed"

        tampered["mino"]["schema_persisted"] = True
        write_json_atomic(client_manifest, tampered)
        client_schema = next(
            (client_manifest.parent / "schema-store" / "schemas").glob("*.schema")
        )
        client_schema.unlink()
        missing_artifact_final = root / "missing-artifact-final.json"
        assert finalize_evidence(
            server_manifest_path=server_manifest,
            client_manifest_path=client_manifest,
            server_build_root=build_roots["server"],
            client_build_root=build_roots["client"],
            output_path=missing_artifact_final,
            expected_commit=commit,
        ) == 1
        missing_artifact = _load_json_object(
            missing_artifact_final, "missing-artifact self-test final manifest"
        )
        assert missing_artifact["outcome"] == "failed"
        assert any("schema artifact" in error for error in missing_artifact["errors"])

        slow_manifest = root / "slow" / "manifest.json"
        started = time.monotonic()
        assert run_role(
            role="client",
            binary=fake,
            address="slow.invalid",
            advertise_address="10.0.0.2",
            port=43191,
            timeout_seconds=1,
            token=token,
            commit=commit,
            manifest_path=slow_manifest,
            log_path=slow_manifest.parent / "client.log",
            allow_short_timeout=True,
            identity_override="b" * 64,
        ) == 1
        assert time.monotonic() - started < 12
        slow = _load_json_object(slow_manifest, "slow self-test manifest")
        assert slow["outcome"] == "failed"
        assert "deadline" in slow["error"]

    try:
        load_token({}, "MISSING_TOKEN")
    except ProtocolError:
        pass
    else:
        raise AssertionError("missing token was accepted")
    print("two-host Mino orchestration self-test: PASS")
