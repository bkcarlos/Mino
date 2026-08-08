#!/usr/bin/env python3
"""Run the schema CodeGen determinism check in two isolated Docker images."""

from __future__ import annotations

import argparse
import base64
import contextlib
import difflib
import io
import os
import re
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class Environment:
    id: str
    image: str
    base_image: str
    compiler_packages: str
    cc: str
    cxx: str
    workspace_path: str
    jobs: int
    extra_bazel_config: str
    timezone: str
    source_date_epoch: int


ENVIRONMENTS = (
    Environment(
        id="ubuntu22-gcc12",
        image="mino-schema-codegen:ubuntu22-gcc12",
        base_image=(
            "ubuntu:22.04@sha256:"
            "0e0a0fc6d18feda9db1590da249ac93e8d5abfea8f4c3c0c849ce512b5ef8982"
        ),
        compiler_packages="gcc-12 g++-12",
        cc="gcc-12",
        cxx="g++-12",
        workspace_path="/workspace-gcc",
        jobs=1,
        extra_bazel_config="gcc12",
        timezone="UTC",
        source_date_epoch=0,
    ),
    Environment(
        id="ubuntu24-clang18",
        image="mino-schema-codegen:ubuntu24-clang18",
        base_image=(
            "ubuntu:24.04@sha256:"
            "4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90"
        ),
        compiler_packages="clang-18",
        cc="clang-18",
        cxx="clang++-18",
        workspace_path="/nested/workspace-clang",
        jobs=4,
        extra_bazel_config="",
        timezone="Pacific/Auckland",
        source_date_epoch=2147483647,
    ),
)


_CREDENTIAL_URL_RE = re.compile(
    r"(?i)(?P<scheme>https?://)[^\s/?#]*@"
)
_PROXY_ENVIRONMENT_NAMES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
)


def _redact(text: str) -> str:
    """Remove URL userinfo before text reaches logs or exceptions."""
    return _CREDENTIAL_URL_RE.sub(r"\g<scheme><redacted>@", text)


def _run(
    command: list[str], *, cwd: Path, environment: dict[str, str] | None = None
) -> None:
    printable = _redact(shlex.join(command))
    print(f"+ {printable}", flush=True)
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
    except OSError as error:
        raise RuntimeError(
            _redact(f"unable to start command {printable}: {error}")
        ) from None

    assert process.stdout is not None
    for line in process.stdout:
        sys.stdout.write(_redact(line))
        sys.stdout.flush()
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(
            f"command failed with exit code {return_code}: {printable}"
        )


def _check_docker(workspace: Path) -> None:
    command = ["docker", "info", "--format", "{{.Architecture}}"]
    try:
        result = subprocess.run(
            command,
            cwd=workspace,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.CalledProcessError) as error:
        details = getattr(error, "stderr", "") or str(error)
        raise RuntimeError(
            _redact(f"unable to query Docker architecture: {details.strip()}")
        ) from None
    architecture = result.stdout.strip()
    if architecture not in ("amd64", "x86_64"):
        raise RuntimeError(
            "the pinned Bazelisk Docker image currently supports amd64 only; "
            f"Docker reported {architecture!r}"
        )


def _proxy_candidates() -> list[str]:
    candidates: list[str] = []
    for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        value = os.environ.get(name)
        if value:
            candidates.append(value)

    config_paths = (
        Path("/etc/proxychains4.conf"),
        Path("/etc/proxychains.conf"),
        Path("/opt/homebrew/etc/proxychains.conf"),
        Path("/usr/local/etc/proxychains.conf"),
    )
    entry = re.compile(r"^\s*(?:http|socks4|socks5)\s+(\S+)\s+(\d+)\s*$")
    for path in config_paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (FileNotFoundError, PermissionError, UnicodeDecodeError):
            continue
        for line in lines:
            match = entry.match(line.split("#", 1)[0])
            if match:
                # Mihomo/Clash commonly exposes one mixed HTTP+SOCKS port. Probe
                # HTTP CONNECT below instead of trusting the proxychains label.
                candidates.append(f"http://{match.group(1)}:{match.group(2)}")

    for port in (7890, 1080, 8080):
        candidates.append(f"http://127.0.0.1:{port}")
    return list(dict.fromkeys(candidates))


def _supports_http_connect(proxy: str) -> bool:
    parsed = urlparse(proxy if "://" in proxy else f"http://{proxy}")
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return False
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError:
        return False

    headers = [
        "CONNECT bcr.bazel.build:443 HTTP/1.1",
        "Host: bcr.bazel.build:443",
        "Proxy-Connection: close",
    ]
    if parsed.username is not None:
        credentials = (
            f"{unquote(parsed.username)}:{unquote(parsed.password or '')}"
        ).encode("utf-8")
        authorization = base64.b64encode(credentials).decode("ascii")
        headers.append(f"Proxy-Authorization: Basic {authorization}")
    request = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii")

    try:
        connection = socket.create_connection((parsed.hostname, port), timeout=3)
        if parsed.scheme == "https":
            context = ssl.create_default_context()
            connection = context.wrap_socket(
                connection, server_hostname=parsed.hostname
            )
        with connection:
            connection.sendall(request)
            response = connection.recv(1024).lower()
    except OSError:
        return False
    first_line = response.split(b"\r\n", 1)[0]
    return b" 200 " in first_line and b"connection established" in first_line


def _resolve_proxy(value: str) -> str | None:
    if value.lower() in ("none", "off", "direct"):
        return None
    if value != "auto":
        explicit = value if "://" in value else f"http://{value}"
        if not _supports_http_connect(explicit):
            raise RuntimeError(
                _redact(f"proxy does not support HTTPS CONNECT: {explicit}")
            )
        return explicit
    for candidate in _proxy_candidates():
        if _supports_http_connect(candidate):
            return candidate
    return None


def _build_image(
    workspace: Path,
    environment: Environment,
    pull: bool,
    proxy: str | None,
    base_image: str,
) -> None:
    command = [
        "docker",
        "build",
    ]
    build_environment = os.environ.copy()
    if proxy is not None:
        # Values stay in the child environment. Docker's predefined proxy build
        # args are requested without values so credentials never enter argv,
        # build history, or the printed command.
        for name in _PROXY_ENVIRONMENT_NAMES:
            build_environment[name] = proxy
        command.extend(["--network=host"])
        for name in _PROXY_ENVIRONMENT_NAMES:
            command.extend(["--build-arg", name])
    command.extend(
        [
            "--file",
            "tools/ci/docker/schema-codegen.Dockerfile",
            "--tag",
            environment.image,
            "--build-arg",
            f"BASE_IMAGE={base_image}",
            "--build-arg",
            f"COMPILER_PACKAGES={environment.compiler_packages}",
            "--build-arg",
            f"CC={environment.cc}",
            "--build-arg",
            f"CXX={environment.cxx}",
            "--build-arg",
            f"WORKSPACE_PATH={environment.workspace_path}",
        ]
    )
    if pull:
        command.append("--pull")
    command.append(".")
    _run(command, cwd=workspace, environment=build_environment)


def _prepare_result_directory(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeError(f"refusing symlink result directory: {path}")
    if path.exists():
        if not path.is_dir():
            raise RuntimeError(f"result path is not a directory: {path}")
        shutil.rmtree(path)
    path.mkdir(parents=True)


def _run_environment(
    workspace: Path, output: Path, environment: Environment
) -> None:
    result_directory = output / environment.id
    _prepare_result_directory(result_directory)
    mount = f"type=bind,src={result_directory},dst=/results"
    command = [
        "docker",
        "run",
        "--rm",
        "--network=none",
        "--env",
        f"MINO_CODEGEN_JOBS={environment.jobs}",
        "--env",
        f"MINO_BAZEL_EXTRA_CONFIG={environment.extra_bazel_config}",
        "--env",
        f"TZ={environment.timezone}",
        "--env",
        f"SOURCE_DATE_EPOCH={environment.source_date_epoch}",
        "--mount",
        mount,
        environment.image,
        "bash",
        "tools/ci/run_schema_codegen_environment.sh",
    ]
    _run(command, cwd=workspace)


def _read_evidence(output: Path, environment: Environment) -> tuple[str, str]:
    root = output / environment.id / "hermetic-codegen"
    manifest_path = root / "SHA256SUMS"
    hash_path = root / "ROOT_SHA256"
    if not manifest_path.is_file() or not hash_path.is_file():
        raise RuntimeError(f"Docker environment did not produce evidence: {root}")
    return (
        manifest_path.read_text(encoding="ascii"),
        hash_path.read_text(encoding="ascii").strip(),
    )


def _compare(output: Path) -> str:
    left_manifest, left_hash = _read_evidence(output, ENVIRONMENTS[0])
    right_manifest, right_hash = _read_evidence(output, ENVIRONMENTS[1])
    if left_manifest != right_manifest:
        diff = difflib.unified_diff(
            left_manifest.splitlines(),
            right_manifest.splitlines(),
            fromfile=f"{ENVIRONMENTS[0].id}/SHA256SUMS",
            tofile=f"{ENVIRONMENTS[1].id}/SHA256SUMS",
            lineterm="",
        )
        sys.stderr.write("\n".join(diff) + "\n")
        raise RuntimeError("Docker CodeGen manifests differ")
    if left_hash != right_hash:
        raise RuntimeError(
            "Docker CodeGen root hashes differ despite equal manifests: "
            f"{left_hash} != {right_hash}"
        )
    if len(left_manifest.splitlines()) != 15:
        raise RuntimeError("Docker CodeGen manifest does not contain 15 outputs")
    return left_hash


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        manifest = "a" * 64 + "  generated/a\n"
        for environment in ENVIRONMENTS:
            root = output / environment.id / "hermetic-codegen"
            root.mkdir(parents=True)
            (root / "SHA256SUMS").write_text(manifest * 15, encoding="ascii")
            (root / "ROOT_SHA256").write_text("b" * 64 + "\n", encoding="ascii")
        assert _compare(output) == "b" * 64

    credential_url = "http://user:token@proxy.example:8080"
    redacted = _redact(
        f"Docker build proxy: {credential_url}; error while using {credential_url}"
    )
    assert redacted == (
        "Docker build proxy: http://<redacted>@proxy.example:8080; "
        "error while using http://<redacted>@proxy.example:8080"
    )
    assert "user" not in redacted
    assert "token" not in redacted
    assert _redact("https://plain.example/path") == "https://plain.example/path"

    captured_log = io.StringIO()
    with contextlib.redirect_stdout(captured_log):
        _run(
            [sys.executable, "-c", f"print({credential_url!r})"],
            cwd=Path.cwd(),
        )
        try:
            _run(
                [sys.executable, "-c", "raise SystemExit(7)", credential_url],
                cwd=Path.cwd(),
            )
        except RuntimeError as error:
            assert "user" not in str(error)
            assert "token" not in str(error)
        else:
            raise AssertionError("failing command did not raise RuntimeError")
    assert "user" not in captured_log.getvalue()
    assert "token" not in captured_log.getvalue()

    requests: list[bytes] = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        proxy_port = listener.getsockname()[1]

        def serve_connect() -> None:
            connection, _ = listener.accept()
            with connection:
                request = b""
                while b"\r\n\r\n" not in request:
                    request += connection.recv(4096)
                requests.append(request)
                connection.sendall(
                    b"HTTP/1.1 200 Connection established\r\n\r\n"
                )

        server = threading.Thread(target=serve_connect, daemon=True)
        server.start()
        authenticated_proxy = f"http://user:token@127.0.0.1:{proxy_port}"
        assert _supports_http_connect(authenticated_proxy)
        server.join(timeout=3)
        assert not server.is_alive()
    assert b"Proxy-Authorization: Basic dXNlcjp0b2tlbg==" in requests[0]
    print("run_schema_codegen_docker_check.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("/tmp/mino-schema-codegen-docker"),
        help="host directory for manifests, generated bytes, and provenance",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse the two previously built Docker images",
    )
    parser.add_argument(
        "--pull",
        action="store_true",
        help="explicitly refresh digest-pinned base images before building",
    )
    parser.add_argument(
        "--ubuntu22-base-image",
        default=ENVIRONMENTS[0].base_image,
        help="explicit Ubuntu 22.04 base image override (default is digest-pinned)",
    )
    parser.add_argument(
        "--ubuntu24-base-image",
        default=ENVIRONMENTS[1].base_image,
        help="explicit Ubuntu 24.04 base image override (default is digest-pinned)",
    )
    parser.add_argument(
        "--proxy",
        default="auto",
        help=(
            "HTTP CONNECT proxy URL, 'auto' to detect proxychains/mixed ports, "
            "or 'none'"
        ),
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return 0

    workspace = Path(__file__).resolve().parents[2]
    if not (workspace / "MODULE.bazel").is_file():
        parser.error(f"cannot locate Mino workspace from {__file__}")
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if output.is_symlink() or not output.is_dir():
        parser.error(f"output is not a real directory: {output}")

    _check_docker(workspace)
    try:
        proxy = _resolve_proxy(args.proxy)
    except RuntimeError as error:
        parser.error(_redact(str(error)))
    print(
        _redact(f"Docker build proxy: {proxy or 'direct'}"),
        flush=True,
    )
    if not args.skip_build:
        base_images = (args.ubuntu22_base_image, args.ubuntu24_base_image)
        for environment, base_image in zip(ENVIRONMENTS, base_images):
            _build_image(
                workspace,
                environment,
                pull=args.pull,
                proxy=proxy,
                base_image=base_image,
            )
    for environment in ENVIRONMENTS:
        _run_environment(workspace, output, environment)

    root_hash = _compare(output)
    print("Schema Docker CodeGen comparison: PASS")
    print(f"outputs=15 root_sha256={root_hash}")
    print(f"evidence={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
