#!/usr/bin/env python3
"""Exercise mino-deploy's real TLS credential preflight expected-failure path."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile


EXPECTED_EXIT = 4
EXPECTED_MESSAGE = "TLS credentials must be bounded, single-link regular files"


class ProbeError(RuntimeError):
    pass


def run(command: list[str], workspace: Path, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=workspace,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ProbeError(f"cannot execute {command[0]}: {error}") from error


def checked(command: list[str], workspace: Path) -> str:
    result = run(command, workspace)
    if result.returncode != 0:
        raise ProbeError(
            f"command failed with exit {result.returncode}: {' '.join(command)}\n{result.stdout}"
        )
    return result.stdout


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--bazel", default="bazel")
    args = parser.parse_args(argv)
    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        print("credential drill: invalid workspace", file=sys.stderr)
        return 70

    try:
        checked([args.bazel, "build", "//tools/deployment:mino_deploy"], workspace)
        bazel_bin_text = checked([args.bazel, "info", "bazel-bin"], workspace)
        bazel_bin = Path(bazel_bin_text.strip().splitlines()[-1])
        deploy = bazel_bin / "tools/deployment/mino_deploy"
        if not deploy.is_file():
            raise ProbeError(f"built mino_deploy is missing: {deploy}")

        generated = checked(
            [
                str(deploy),
                "generate",
                "--environment",
                "production",
                "--role",
                "edge",
                "--node-id",
                "617",
                "--security-domain-id",
                "17",
                "--region-id",
                "617",
            ],
            workspace,
        )
        with tempfile.TemporaryDirectory(prefix="mino-tls-preflight-") as temporary:
            root = Path(temporary).resolve()
            data = root / "data"
            runtime = root / "runtime"
            schemas = root / "schemas"
            secrets = root / "secrets"
            for directory in (data, runtime, schemas, secrets):
                directory.mkdir(mode=0o700)
            ca = secrets / "ca.pem"
            certificate = secrets / "tls.crt"
            private_key = secrets / "tls.key"
            ca.write_text("drill trust anchor placeholder\n", encoding="ascii")
            certificate.write_text("drill certificate placeholder\n", encoding="ascii")
            private_key.touch(mode=0o600)
            os.chmod(ca, 0o600)
            os.chmod(certificate, 0o600)
            os.chmod(private_key, 0o600)

            replacements = {
                "/run/secrets/mino/ca.pem": str(ca),
                "/run/secrets/mino/tls.crt": str(certificate),
                "/run/secrets/mino/tls.key": str(private_key),
                "/var/lib/mino/data": str(data),
                "/run/mino": str(runtime),
                "/var/lib/mino/schemas": str(schemas),
                "min_free_bytes = 536870912": "min_free_bytes = 1048576",
            }
            config_text = generated
            for old, new in replacements.items():
                if old not in config_text:
                    raise ProbeError(f"generated deployment contract no longer contains {old!r}")
                config_text = config_text.replace(old, new)
            config = root / "node.toml"
            config.write_text(config_text, encoding="utf-8")
            os.chmod(config, 0o600)

            result = run(
                [str(deploy), "preflight", "--config", str(config)], workspace, 30
            )
            sys.stdout.write(result.stdout)
            if result.returncode != EXPECTED_EXIT or EXPECTED_MESSAGE not in result.stdout:
                print(
                    "credential drill: preflight did not fail for the empty private key "
                    f"as expected (exit={result.returncode})",
                    file=sys.stderr,
                )
                return 70
            print(
                "DRILL_EXPECTED_FAILURE scenario=tls-credential-invalid "
                f"observed_exit={result.returncode} cleanup=temporary-directory"
            )
            return EXPECTED_EXIT
    except ProbeError as error:
        print(f"credential drill: {error}", file=sys.stderr)
        return 70


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
