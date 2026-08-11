#!/usr/bin/env python3
"""Bounded dry-run and oneshot smoke for the real node supervisor."""

import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request

RUNFILES = Path(os.environ["TEST_SRCDIR"]) / os.environ.get("TEST_WORKSPACE", "mino")
NODE = RUNFILES / "tools/deployment/mino_node"
CONFIG = RUNFILES / "configs/node.container-recorder.toml"
DEPLOY = RUNFILES / "tools/deployment/mino_deploy"


def run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments, check=False, capture_output=True, text=True, timeout=30
    )


class NodeSupervisorSmokeTest(unittest.TestCase):
    def test_dry_run_and_oneshot_are_real_and_bounded(self) -> None:
        dry_run = run(str(NODE), "--config", str(CONFIG), "--dry-run")
        self.assertEqual(dry_run.returncode, 0, dry_run.stderr)
        self.assertIn("LocalBus + Monitoring + Recorder", dry_run.stdout)
        self.assertIn("RemoteBridge disabled", dry_run.stdout)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            runtime = root / "run"
            schemas = root / "schemas"
            for directory in (data, runtime, schemas):
                directory.mkdir()
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            policy = root / "security-domains.toml"
            policy.write_text(
                "schema_version = 1\n\n[[domains]]\n"
                "security_domain_id = 7\ntrusted = false\n"
                f"uid = {os.geteuid()}\ngid = {os.getegid()}\n"
                'namespace = "smoke-domain-7"\n',
                encoding="utf-8",
            )
            policy.chmod(0o400)
            attestation = root / "security-domain.namespace"
            attestation.write_text("smoke-domain-7\n", encoding="utf-8")
            attestation.chmod(0o400)

            text = CONFIG.read_text(encoding="utf-8")
            replacements = {
                "bytes = 536870912": "bytes = 8388608",
                "memory_bytes = 1073741824": "memory_bytes = 16777216",
                "shm_bytes = 536870912": "shm_bytes = 8388608",
                "file_descriptors = 8192": "file_descriptors = 64",
                "threads = 256": "threads = 8",
                "port = 9464": f"port = {port}",
                "uid = 65532": f"uid = {os.geteuid()}",
                "gid = 65532": f"gid = {os.getegid()}",
                'namespace = "mino-domain-7"': 'namespace = "smoke-domain-7"',
                'policy_file = "/etc/mino/security-domains.toml"': f'policy_file = "{policy}"',
                'namespace_attestation_file = "/run/secrets/mino/security-domain.namespace"': f'namespace_attestation_file = "{attestation}"',
                'data_dir = "/var/lib/mino/data"': f'data_dir = "{data}"',
                'runtime_dir = "/run/mino"': f'runtime_dir = "{runtime}"',
                'schema_dir = "/var/lib/mino/schemas"': f'schema_dir = "{schemas}"',
                "min_free_bytes = 536870912": "min_free_bytes = 1048576",
            }
            for old, new in replacements.items():
                self.assertIn(old, text)
                text = text.replace(old, new, 1)
            config = root / "node.toml"
            config.write_text(text, encoding="utf-8")
            config.chmod(0o400)
            oneshot = run(str(NODE), "--config", str(config), "--oneshot")
            self.assertEqual(oneshot.returncode, 0, oneshot.stderr)
            self.assertIn("stopped cleanly", oneshot.stdout)
            self.assertTrue((data / "recorder").is_dir())

            if os.geteuid() == 0:
                return
            node_executable = root / "mino-node"
            shutil.copyfile(NODE, node_executable)
            node_executable.chmod(0o500)
            process = subprocess.Popen(
                [
                    str(DEPLOY), "start", "--config", str(config), "--",
                    str(node_executable), "--config", str(config),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 10
                healthy = False
                while time.monotonic() < deadline and process.poll() is None:
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}/-/healthy", timeout=0.5
                        ) as response:
                            healthy = response.status == 200
                    except (urllib.error.URLError, TimeoutError):
                        time.sleep(0.05)
                    if healthy:
                        break
                if not healthy:
                    stdout, stderr = process.communicate(timeout=5)
                    self.fail(
                        "supervised node did not become healthy; "
                        f"returncode={process.returncode} stdout={stdout} stderr={stderr}"
                    )
                process.terminate()
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 143, stderr)
                self.assertIn("mino-node stopped cleanly", stdout)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
