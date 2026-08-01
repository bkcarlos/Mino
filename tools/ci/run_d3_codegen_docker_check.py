#!/usr/bin/env python3
"""Run the D3 CodeGen determinism check in two isolated Docker images."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import difflib
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


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
    timezone: str
    source_date_epoch: int


ENVIRONMENTS = (
    Environment(
        id="ubuntu22-gcc12",
        image="mino-d3-codegen:ubuntu22-gcc12",
        base_image="ubuntu:22.04",
        compiler_packages="gcc-12 g++-12",
        cc="gcc-12",
        cxx="g++-12",
        workspace_path="/workspace-gcc",
        jobs=1,
        timezone="UTC",
        source_date_epoch=0,
    ),
    Environment(
        id="ubuntu24-clang18",
        image="mino-d3-codegen:ubuntu24-clang18",
        base_image="ubuntu:24.04",
        compiler_packages="clang-18",
        cc="clang-18",
        cxx="clang++-18",
        workspace_path="/nested/workspace-clang",
        jobs=4,
        timezone="Pacific/Auckland",
        source_date_epoch=2147483647,
    ),
)


def _run(command: list[str], *, cwd: Path) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def _check_docker(workspace: Path) -> None:
    result = subprocess.run(
        ["docker", "info", "--format", "{{.Architecture}}"],
        cwd=workspace,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    architecture = result.stdout.strip()
    if architecture not in ("amd64", "x86_64"):
        raise RuntimeError(
            "the pinned Bazelisk Docker image currently supports amd64 only; "
            f"Docker reported {architecture!r}"
        )


def _build_image(workspace: Path, environment: Environment, pull: bool) -> None:
    command = [
        "docker",
        "build",
        "--file",
        "tools/ci/docker/d3-codegen.Dockerfile",
        "--tag",
        environment.image,
        "--build-arg",
        f"BASE_IMAGE={environment.base_image}",
        "--build-arg",
        f"COMPILER_PACKAGES={environment.compiler_packages}",
        "--build-arg",
        f"CC={environment.cc}",
        "--build-arg",
        f"CXX={environment.cxx}",
        "--build-arg",
        f"WORKSPACE_PATH={environment.workspace_path}",
    ]
    if pull:
        command.append("--pull")
    command.append(".")
    _run(command, cwd=workspace)


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
        f"TZ={environment.timezone}",
        "--env",
        f"SOURCE_DATE_EPOCH={environment.source_date_epoch}",
        "--mount",
        mount,
        environment.image,
        "bash",
        "tools/ci/run_d3_codegen_environment.sh",
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
    print("run_d3_codegen_docker_check.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("/tmp/mino-d3-codegen-docker"),
        help="host directory for manifests, generated bytes, and provenance",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse the two previously built Docker images",
    )
    parser.add_argument(
        "--no-pull",
        action="store_true",
        help="do not refresh the pinned Ubuntu image tags",
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
    if not args.skip_build:
        for environment in ENVIRONMENTS:
            _build_image(workspace, environment, pull=not args.no_pull)
    for environment in ENVIRONMENTS:
        _run_environment(workspace, output, environment)

    root_hash = _compare(output)
    print("D3 Docker CodeGen comparison: PASS")
    print(f"outputs=15 root_sha256={root_hash}")
    print(f"evidence={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
