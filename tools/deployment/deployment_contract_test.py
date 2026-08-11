#!/usr/bin/env python3
"""Deployment CLI, startup dry-run, secret, and container contract tests."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


RUNFILES = Path(os.environ["TEST_SRCDIR"]) / os.environ.get("TEST_WORKSPACE", "mino")
DEPLOY = RUNFILES / "tools/deployment/mino_deploy"
START_SCRIPT = RUNFILES / "tools/deployment/mino-node-start"
GOLDEN = RUNFILES / "configs/node.production-edge.toml"
DOCKERFILE = RUNFILES / "tools/deployment/Dockerfile"
DOCKERIGNORE = RUNFILES / ".dockerignore"

CONTAINER_CONFIG = RUNFILES / "configs/node.container-recorder.toml"
ISOLATION_POLICY = RUNFILES / "configs/security-domains.toml"
IMAGE_CONTRACT = RUNFILES / "tools/deployment/image-contract.json"
PROVENANCE_TOOL = RUNFILES / "tools/deployment/emit_image_provenance.py"
BUILD_IMAGE_SCRIPT = RUNFILES / "tools/deployment/build-image.sh"
NODE_SOURCE = RUNFILES / "tools/deployment/node_supervisor.cc"


def run(*arguments: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(argument) for argument in arguments],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )


class DeploymentContractTest(unittest.TestCase):
    def test_generation_is_deterministic_and_matches_golden(self) -> None:
        command = (
            str(DEPLOY),
            "generate",
            "--environment",
            "production",
            "--role",
            "edge",
            "--node-id",
            "1001",
            "--security-domain-id",
            "7",
            "--region-id",
            "17",
            "--service-uid",
            "65532",
            "--service-gid",
            "65532",
            "--namespace",
            "mino-domain-7",
        )
        first = run(*command)
        second = run(*command)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(first.stdout, GOLDEN.read_text(encoding="utf-8"))
        self.assertNotIn("-----BEGIN", first.stdout)

    def test_every_environment_and_role_template_is_valid(self) -> None:
        for environment in ("development", "staging", "production"):
            for role in ("core", "edge", "recorder"):
                generated = run(
                    str(DEPLOY),
                    "generate",
                    "--environment",
                    environment,
                    "--role",
                    role,
                    "--node-id",
                    "9",
                    "--security-domain-id",
                    "8",
                    "--region-id",
                    "7",
                    "--service-uid",
                    "65532",
                    "--service-gid",
                    "65532",
                    "--namespace",
                    "test-domain-8",
                )
                self.assertEqual(generated.returncode, 0, generated.stderr)
                self.assertIn(f'environment = "{environment}"', generated.stdout)
                self.assertIn(f'role = "{role}"', generated.stdout)
                self.assertNotIn("-----BEGIN", generated.stdout)
                if role == "recorder":
                    self.assertIn("bridge_connections = 0", generated.stdout)
                    self.assertIn("enabled = false", generated.stdout)

    def test_validate_rejects_unknown_keys_with_documented_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            valid_config = Path(temporary) / "valid.toml"
            valid_config.write_text(GOLDEN.read_text(encoding="utf-8"), encoding="utf-8")
            valid_config.chmod(0o400)
            valid = run(str(DEPLOY), "validate", "--config", str(valid_config))
            self.assertEqual(valid.returncode, 0, valid.stderr)
            container_config = Path(temporary) / "container.toml"
            container_config.write_text(
                CONTAINER_CONFIG.read_text(encoding="utf-8"), encoding="utf-8"
            )
            container_config.chmod(0o400)
            container_valid = run(
                str(DEPLOY), "validate", "--config", str(container_config)
            )
            self.assertEqual(container_valid.returncode, 0, container_valid.stderr)

            bad = Path(temporary) / "bad.toml"
            bad.write_text(
                GOLDEN.read_text(encoding="utf-8") + "\nunknown = true\n",
                encoding="utf-8",
            )
            bad.chmod(0o400)
            rejected = run(str(DEPLOY), "validate", "--config", str(bad))
        self.assertEqual(rejected.returncode, 3)
        self.assertIn("unknown key", rejected.stderr)

    def test_start_script_dry_run_checks_permissions_without_starting_child(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            runtime = root / "run"
            schemas = root / "schemas"
            secrets = root / "secrets"
            for directory in (data, runtime, schemas, secrets):
                directory.mkdir()
                directory.chmod(0o700)

            policy = root / "security-domains.toml"
            policy.write_text(
                "schema_version = 1\n\n[[domains]]\n"
                "security_domain_id = 7\ntrusted = false\n"
                f"uid = {os.geteuid()}\ngid = {os.getegid()}\n"
                'namespace = "test-domain-7"\n',
                encoding="utf-8",
            )
            policy.chmod(0o400)
            namespace_attestation = secrets / "security-domain.namespace"
            namespace_attestation.write_text("test-domain-7\n", encoding="utf-8")
            namespace_attestation.chmod(0o400)

            ca = secrets / "ca.pem"
            certificate = secrets / "tls.crt"
            key = secrets / "tls.key"
            ca.write_text("test trust reference\n", encoding="utf-8")
            certificate.write_text("test certificate reference\n", encoding="utf-8")
            key.write_text("test key reference\n", encoding="utf-8")
            ca.chmod(0o400)
            certificate.chmod(0o400)
            key.chmod(0o440)

            executable = root / "node-under-test"
            shutil.copyfile(DEPLOY, executable)
            executable.chmod(0o500)

            text = GOLDEN.read_text(encoding="utf-8")
            replacements = {
                "bytes = 536870912": "bytes = 1048576",
                "memory_bytes = 1073741824": "memory_bytes = 1048576",
                "shm_bytes = 536870912": "shm_bytes = 1048576",
                "file_descriptors = 8192": "file_descriptors = 64",
                "threads = 256": "threads = 1",
                "bridge_connections = 64": "bridge_connections = 1",
                "max_connections = 64": "max_connections = 1",
                "uid = 65532": f"uid = {os.geteuid()}",
                "gid = 65532": f"gid = {os.getegid()}",
                'namespace = "mino-domain-7"': 'namespace = "test-domain-7"',
                'policy_file = "/etc/mino/security-domains.toml"': f'policy_file = "{policy}"',
                'namespace_attestation_file = "/run/secrets/mino/security-domain.namespace"': f'namespace_attestation_file = "{namespace_attestation}"',
                'data_dir = "/var/lib/mino/data"': f'data_dir = "{data}"',
                'runtime_dir = "/run/mino"': f'runtime_dir = "{runtime}"',
                'schema_dir = "/var/lib/mino/schemas"': f'schema_dir = "{schemas}"',
                "min_free_bytes = 536870912": "min_free_bytes = 1048576",
                'trust_anchors_file = "/run/secrets/mino/ca.pem"': f'trust_anchors_file = "{ca}"',
                'certificate_chain_file = "/run/secrets/mino/tls.crt"': f'certificate_chain_file = "{certificate}"',
                'private_key_file = "/run/secrets/mino/tls.key"': f'private_key_file = "{key}"',
            }
            for old, new in replacements.items():
                self.assertIn(old, text)
                text = text.replace(old, new, 1)
            config = root / "node.toml"
            config.write_text(text, encoding="utf-8")
            config.chmod(0o400)

            environment = os.environ.copy()
            environment["MINO_DEPLOY_BIN"] = str(DEPLOY)
            environment["MINO_NODE_CONFIG"] = str(config)

            policy.chmod(0o600)
            policy.write_text(
                "schema_version = 1\n\n[[domains]]\n"
                "security_domain_id = 7\ntrusted = false\n"
                f"uid = {os.geteuid() + 1}\ngid = {os.getegid()}\n"
                'namespace = "test-domain-7"\n',
                encoding="utf-8",
            )
            policy.chmod(0o400)
            identity_rejected = run(
                str(START_SCRIPT), "--dry-run", str(executable), env=environment
            )
            self.assertEqual(identity_rejected.returncode, 4)
            self.assertIn("disagrees", identity_rejected.stderr)
            policy.chmod(0o600)
            policy.write_text(
                "schema_version = 1\n\n[[domains]]\n"
                "security_domain_id = 7\ntrusted = false\n"
                f"uid = {os.geteuid()}\ngid = {os.getegid()}\n"
                'namespace = "test-domain-7"\n',
                encoding="utf-8",
            )
            policy.chmod(0o400)

            rejected = run(
                str(START_SCRIPT), "--dry-run", str(executable), env=environment
            )
            self.assertEqual(rejected.returncode, 4)
            self.assertIn("private key permissions", rejected.stderr)

            key.chmod(0o400)
            accepted = run(
                str(START_SCRIPT), "--dry-run", str(executable), env=environment
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            self.assertIn("dry-run ok", accepted.stdout)



    def test_supervisor_source_assembles_real_components_and_fails_closed(self) -> None:
        source = NODE_SOURCE.read_text(encoding="utf-8")
        self.assertIn("LocalBusDeployment::Create", source)
        self.assertIn("MonitoringDeployment::Create", source)
        self.assertIn("storage::Recorder::Create", source)
        self.assertIn("--oneshot", source)
        self.assertIn("does not assemble RemoteBridge", source)

    def test_container_contract_is_non_root_read_only_compatible_and_mount_only(self) -> None:
        dockerfile = DOCKERFILE.read_text(encoding="utf-8")
        self.assertGreaterEqual(dockerfile.count("FROM "), 2)
        self.assertIn("ARG BUILD_IMAGE\n", dockerfile)
        self.assertIn("ARG RUNTIME_IMAGE\n", dockerfile)
        self.assertNotIn("ARG BUILD_IMAGE=", dockerfile)
        self.assertNotIn("ARG RUNTIME_IMAGE=", dockerfile)
        self.assertIn('case "${BUILD_IMAGE}" in *@sha256:*', dockerfile)
        self.assertIn('case "${RUNTIME_IMAGE}" in *@sha256:*', dockerfile)
        self.assertIn("RUN bazel build --config=release --lockfile_mode=error", dockerfile)
        self.assertIn("//tools/mino:mino", dockerfile)
        self.assertIn("//tools/deployment:mino_deploy", dockerfile)
        self.assertIn("//tools/deployment:mino_node", dockerfile)
        self.assertIn("/usr/local/bin/mino-deploy", dockerfile)
        self.assertIn("/usr/local/bin/mino-node", dockerfile)
        self.assertIn("USER 65532:65532", dockerfile)
        self.assertNotIn("VOLUME ", dockerfile)
        self.assertIn("HEALTHCHECK", dockerfile)
        self.assertIn('ENTRYPOINT ["/usr/local/bin/mino-deploy"]', dockerfile)
        self.assertIn('CMD ["start", "--config", "/etc/mino/node.toml"', dockerfile)
        self.assertNotIn('CMD ["validate"', dockerfile)
        self.assertIn("org.opencontainers.image.revision", dockerfile)
        self.assertIn("org.mino.sbom.format", dockerfile)
        copy_lines = [line for line in dockerfile.splitlines() if line.startswith("COPY")]
        self.assertFalse(any(".key" in line or ".pem" in line or ".crt" in line for line in copy_lines))
        ignored = DOCKERIGNORE.read_text(encoding="utf-8")
        for pattern in ("**/*.key", "**/*.pem", "**/*.p12", "**/secrets/**"):
            self.assertIn(pattern, ignored)

    def test_deployable_artifacts_contain_no_inline_secret_material(self) -> None:
        for artifact in (GOLDEN, CONTAINER_CONFIG, ISOLATION_POLICY, DOCKERFILE, START_SCRIPT):
            contents = artifact.read_text(encoding="utf-8").lower()
            self.assertNotIn("-----begin private key-----", contents, artifact)
            self.assertNotIn("-----begin encrypted private key-----", contents, artifact)
            self.assertNotIn("private_key_pem", contents, artifact)

    def test_machine_readable_contract_and_sbom_provenance(self) -> None:
        contract = json.loads(IMAGE_CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual(contract["identity"], {"uid": 65532, "gid": 65532, "root_forbidden": True})
        self.assertTrue(contract["filesystem"]["read_only_root_required"])
        self.assertEqual(contract["default_process"]["command"][0], "start")
        self.assertIn("SPDX", contract["supply_chain"]["sbom"])
        script = BUILD_IMAGE_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("syft scan", script)
        self.assertIn("--provenance=mode=max", script)
        self.assertIn("image_digest=", script)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            metadata = root / "metadata.json"
            iid = root / "iid"
            sbom = root / "mino.spdx.json"
            output = root / "provenance.json"
            metadata.write_text('{"containerimage.digest":"sha256:' + "a" * 64 + '"}', encoding="utf-8")
            iid.write_text("sha256:" + "b" * 64 + "\n", encoding="utf-8")
            sbom.write_text('{"spdxVersion":"SPDX-2.3"}\n', encoding="utf-8")
            emitted = run(
                "python3", str(PROVENANCE_TOOL),
                "--image-ref", "example/mino:test",
                "--build-image", "example/build@sha256:" + "c" * 64,
                "--runtime-image", "example/runtime@sha256:" + "d" * 64,
                "--revision", "deadbeef", "--created", "2026-01-01T00:00:00Z",
                "--iid-file", str(iid), "--metadata-file", str(metadata),
                "--sbom-file", str(sbom), "--output", str(output),
            )
            self.assertEqual(emitted.returncode, 0, emitted.stderr)
            provenance = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(provenance["image_digest"], "sha256:" + "a" * 64)
            expected = "sha256:" + hashlib.sha256(sbom.read_bytes()).hexdigest()
            self.assertEqual(provenance["sbom"]["digest"], expected)


if __name__ == "__main__":
    unittest.main()
