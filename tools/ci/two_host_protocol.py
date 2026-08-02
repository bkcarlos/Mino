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

PROTOCOL = "mino-two-host-mino-v1"
SCHEMA_VERSION = 2
MIN_TOKEN_BYTES = 32
MIN_PRODUCTION_TIMEOUT_SECONDS = 1800
MAX_TIMEOUT_SECONDS = 3600
COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
HOST_RE = re.compile(
    r"^(?=.{1,253}$)(?!-)(?:[A-Za-z0-9-]{1,63}\.)*"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
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
    for field in (
        "session_discovery",
        "bridge_active",
        "reliable_sent",
        "reliable_received",
        "remote_acknowledged",
    ):
        if result.get(field) is not True:
            raise ProtocolError(f"Mino data-path evidence {field!r} is not true")
    for field in ("local_session_epoch", "remote_session_epoch"):
        value = result.get(field)
        if not isinstance(value, int) or value <= 0:
            raise ProtocolError(f"Mino result {field!r} is not positive")
    role_counter = "accepted_connections" if role == "server" else "connection_attempts"
    counter = result.get(role_counter)
    if not isinstance(counter, int) or counter <= 0:
        raise ProtocolError(f"Mino result {role_counter!r} is not positive")


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
        "binary": {
            "path": binary.name,
            "sha256": binary_hash,
            "size_bytes": binary.stat().st_size,
        },
        "process_exit_code": return_code,
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


def finalize_evidence(
    *,
    server_manifest_path: Path,
    client_manifest_path: Path,
    output_path: Path,
    expected_commit: str | None = None,
) -> int:
    errors: list[str] = []
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
        if manifest.get("protocol") != PROTOCOL or manifest.get("role") != role:
            errors.append(f"{role} manifest protocol or role mismatch")
        if manifest.get("outcome") != "passed":
            errors.append(f"{role} outcome is not passed")

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
    if expected_commit:
        expected_commit = expected_commit.lower()
        if server_commit != expected_commit or client_commit != expected_commit:
            errors.append("one or both role commits differ from expected commit")
    if server_peer.get("commit") != client_commit:
        errors.append("server Mino payload did not corroborate client commit")
    if client_peer.get("commit") != server_commit:
        errors.append("client Mino payload did not corroborate server commit")
    if server_peer.get("identity") != client_local.get("identity"):
        errors.append("server Mino payload did not corroborate client machine identity")
    if client_peer.get("identity") != server_local.get("identity"):
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
    if server_peer.get("advertised_address") != client_local.get("advertised_address"):
        errors.append("server did not receive the client advertised address over Mino")
    if client_peer.get("advertised_address") != server_local.get("advertised_address"):
        errors.append("client did not receive the server advertised address over Mino")

    for role, result in (("server", server_mino), ("client", client_mino)):
        for field in (
            "session_discovery",
            "bridge_active",
            "reliable_sent",
            "reliable_received",
            "remote_acknowledged",
        ):
            if result.get(field) is not True:
                errors.append(f"{role} Mino evidence {field} is not true")

    logs: dict[str, Any] = {}
    role_hashes: dict[str, Any] = {}
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
        logs[role] = {
            "path": str(log_path),
            "sha256": actual_hash,
            "size_bytes": log_path.stat().st_size,
        }

    final = {
        "schema_version": SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "outcome": "passed" if not errors else "failed",
        "generated_at": utc_now(),
        "commits": {"server": server_commit, "client": client_commit},
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
            "identical": binary_identical,
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
        },
        "logs": logs,
        "role_manifests": role_hashes,
        "errors": errors,
        "github": github_provenance(os.environ),
    }
    write_json_atomic(output_path, final)
    print(f"final_manifest={output_path}", flush=True)
    return 0 if not errors else 1


def self_test() -> None:
    token = b"self-test-token-that-is-at-least-thirty-two-bytes"
    commit = "a" * 40
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
    'schema_version': 1,
    'protocol': 'mino-two-host-mino-v1',
    'role': role,
    'outcome': 'passed',
    'commit': args['commit'],
    'machine_identity': args['machine-identity'],
    'peer_commit': args['commit'],
    'peer_machine_identity': peer_identity,
    'local_address': args['advertise-address'],
    'peer_address': peer_address,
    'session_discovery': True,
    'bridge_active': True,
    'reliable_sent': True,
    'reliable_received': True,
    'remote_acknowledged': True,
    'local_session_epoch': 11,
    'remote_session_epoch': 22,
    'connection_attempts': 1 if role == 'client' else 0,
    'accepted_connections': 1 if role == 'server' else 0,
    'elapsed_ms': 1,
    'error': '',
}
path = Path(args['output'])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(result), encoding='utf-8')
print('fake mino probe completed')
""",
            encoding="utf-8",
        )
        fake.chmod(0o755)
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
            output_path=final_path,
            expected_commit=commit,
        ) == 0
        final = _load_json_object(final_path, "self-test final manifest")
        assert final["outcome"] == "passed"
        assert final["binary"]["identical"]
        assert final["mino_data_path"]["server_remote_acknowledged"]
        assert final["mino_data_path"]["client_remote_acknowledged"]

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
