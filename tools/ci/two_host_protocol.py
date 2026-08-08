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
SCHEMA_VERSION = 4
MINO_RESULT_SCHEMA_VERSION = 4
MIN_TOKEN_BYTES = 32
MIN_PRODUCTION_TIMEOUT_SECONDS = 1800
MAX_TIMEOUT_SECONDS = 3600
COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
HOST_RE = re.compile(
    r"^(?=.{1,253}$)(?!-)(?:[A-Za-z0-9-]{1,63}\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
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


def _validate_mino_result(
    result: Mapping[str, Any],
    *,
    role: str,
    commit: str,
    local_identity: str,
) -> None:
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
                )
            except ProtocolError as error:
                errors.append(f"{role} nested Mino result: {error}")
        if result.get("local_address") != local.get("advertised_address"):
            errors.append(f"{role} Mino local address differs from role manifest")
        if result.get("peer_commit") != peer.get("commit"):
            errors.append(f"{role} Mino peer commit differs from role manifest")
        if result.get("peer_machine_identity") != peer.get("identity"):
            errors.append(f"{role} Mino peer identity differs from role manifest")
        if result.get("peer_address") != peer.get("advertised_address"):
            errors.append(f"{role} Mino peer address differs from role manifest")

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
result = {
    'schema_version': 4,
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
        assert final["outcome"] == "passed"
        assert final["binary"]["identical"]
        assert final["binary"]["bound_to_hosted_builds"]
        assert final["mino_data_path"]["server_remote_acknowledged"]
        assert final["mino_data_path"]["client_remote_acknowledged"]
        assert final["reconnect"]["automatic"]
        assert final["reconnect"]["duplicate_suppressed"]
        assert final["schema"]["request"]
        assert final["schema"]["persisted"]

        original_client_manifest = client_manifest.read_bytes()
        client_result_path = client_manifest.parent / "mino-result.json"
        original_client_result = client_result_path.read_bytes()
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
