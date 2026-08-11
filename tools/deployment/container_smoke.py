#!/usr/bin/env python3
"""Optional real-container health, read-only-root, UID, and SIGTERM smoke."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid


def run(*args: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False, timeout=timeout)


def main() -> int:
    image = os.environ.get("MINO_CONTAINER_SMOKE_IMAGE", "")
    if not image:
        print("SKIP: MINO_CONTAINER_SMOKE_IMAGE is not set")
        return 0
    if shutil.which("docker") is None:
        print("SKIP: docker is unavailable")
        return 0

    name = "mino-smoke-" + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory() as temporary:
        attestation = Path(temporary) / "security-domain.namespace"
        attestation.write_text("mino-domain-7\n", encoding="utf-8")
        attestation.chmod(0o444)
        started = run(
            "docker", "run", "--detach", "--name", name,
            "--read-only", "--user", "65532:65532", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges",
            "--tmpfs", "/run/mino:rw,noexec,nosuid,size=64m,mode=700,uid=65532,gid=65532",
            "--tmpfs", "/var/lib/mino/data:rw,noexec,nosuid,size=1g,mode=700,uid=65532,gid=65532",
            "--tmpfs", "/var/lib/mino/schemas:rw,noexec,nosuid,size=64m,mode=700,uid=65532,gid=65532",
            "--mount", f"type=bind,src={attestation},dst=/run/secrets/mino/security-domain.namespace,readonly",
            image,
        )
        if started.returncode != 0:
            print(started.stderr)
            return 1
        try:
            deadline = time.monotonic() + 20
            probe = None
            while time.monotonic() < deadline:
                probe = run(
                    "docker", "exec", name, "/usr/local/bin/mino-deploy",
                    "probe", "--config", "/etc/mino/node.toml", "--kind", "health",
                    timeout=5,
                )
                if probe.returncode == 0:
                    break
                time.sleep(0.25)
            if probe is None or probe.returncode != 0:
                print(run("docker", "logs", name).stdout)
                print("health probe failed:", "" if probe is None else probe.stderr)
                return 1
            identity = run("docker", "exec", name, "id", "-u", timeout=5)
            if identity.returncode == 0 and identity.stdout.strip() != "65532":
                print("container process did not run as UID 65532")
                return 1
            stopped = run("docker", "stop", "--time", "10", name, timeout=15)
            if stopped.returncode != 0:
                print(stopped.stderr)
                return 1
            logs = run("docker", "logs", name).stdout
            if "mino-node stopped cleanly" not in logs:
                print(logs)
                print("graceful SIGTERM evidence was not observed")
                return 1
            print("container smoke ok: health, read-only rootfs, nonroot, SIGTERM")
            return 0
        finally:
            run("docker", "rm", "--force", name, timeout=10)


if __name__ == "__main__":
    raise SystemExit(main())
