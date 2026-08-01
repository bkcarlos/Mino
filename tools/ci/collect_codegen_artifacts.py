#!/usr/bin/env python3
"""Collect deterministic minoc outputs and write content hashes.

The script intentionally hashes workspace-logical paths rather than Bazel's
platform-specific output paths. It has no third-party dependencies.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tempfile
from typing import Iterable


TARGETS = (
    "//tools/minoc:sample_codegen",
    "//tools/minoc:canonical_wire_codegen",
    "//tools/minoc:mangling_codegen",
    "//tools/minoc:sensor_frame_codegen",
    "//mino/schema/fuzz:codegen_golden",
)

EXPECTED_OUTPUTS = frozenset(
    {
        "tools/minoc/tests/generated/sample.generated.h",
        "tools/minoc/tests/generated/sample.generated.cc",
        "tools/minoc/tests/generated/sample.descriptor",
        "tools/minoc/tests/generated/canonical_wire.generated.h",
        "tools/minoc/tests/generated/canonical_wire.generated.cc",
        "tools/minoc/tests/generated/canonical_wire.descriptor",
        "tools/minoc/tests/generated/mangling.generated.h",
        "tools/minoc/tests/generated/mangling.generated.cc",
        "tools/minoc/tests/generated/mangling.descriptor",
        "tools/minoc/tests/generated/sensor_frame.generated.h",
        "tools/minoc/tests/generated/sensor_frame.generated.cc",
        "tools/minoc/tests/generated/sensor_frame.descriptor",
        "mino/schema/fuzz/generated/codegen_golden.generated.h",
        "mino/schema/fuzz/generated/codegen_golden.generated.cc",
        "mino/schema/fuzz/testdata/codegen_golden.descriptor",
    }
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _logical_path(raw_path: str) -> str:
    normalized = raw_path.strip().replace("\\", "/")
    if not normalized:
        raise ValueError("empty Bazel output path")
    parts = PurePosixPath(normalized).parts
    try:
        bazel_out = parts.index("bazel-out")
        bin_index = parts.index("bin", bazel_out + 1)
    except ValueError as error:
        raise ValueError(
            f"output is not below bazel-out/<configuration>/bin: {raw_path!r}"
        ) from error
    logical_parts = parts[bin_index + 1 :]
    if not logical_parts or any(part in ("", ".", "..") for part in logical_parts):
        raise ValueError(f"unsafe logical output path: {raw_path!r}")
    logical = PurePosixPath(*logical_parts)
    if logical.is_absolute():
        raise ValueError(f"absolute logical output path: {raw_path!r}")
    return logical.as_posix()


def _manifest(entries: Iterable[tuple[str, bytes]]) -> bytes:
    ordered = sorted(entries, key=lambda entry: entry[0])
    lines = [f"{_sha256(data)}  {logical}\n" for logical, data in ordered]
    return "".join(lines).encode("utf-8")


def _query_outputs(bazel: str, config: str, workspace: Path) -> dict[str, Path]:
    expression = "set(" + " ".join(TARGETS) + ")"
    command = [
        bazel,
        "cquery",
        expression,
        f"--config={config}",
        "--output=files",
        "--output_groups=default,descriptor",
    ]
    result = subprocess.run(
        command,
        cwd=workspace,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise RuntimeError(f"bazel cquery failed with exit code {result.returncode}")

    outputs: dict[str, Path] = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        logical = _logical_path(line)
        source = Path(line.strip())
        if not source.is_absolute():
            source = workspace / source
        if logical in outputs:
            raise RuntimeError(f"duplicate logical CodeGen output: {logical}")
        outputs[logical] = source

    actual = frozenset(outputs)
    if actual != EXPECTED_OUTPUTS:
        missing = sorted(EXPECTED_OUTPUTS - actual)
        unexpected = sorted(actual - EXPECTED_OUTPUTS)
        raise RuntimeError(
            "unexpected CodeGen output set; "
            f"missing={missing!r}, unexpected={unexpected!r}"
        )
    return outputs


def _collect(outputs: dict[str, Path], destination: Path) -> None:
    destination = Path(os.path.abspath(destination))
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise RuntimeError(f"refusing to replace symlink destination: {destination}")

    staging: Path | None = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent)
    )
    try:
        artifact_root = staging / "artifacts"
        entries: list[tuple[str, bytes]] = []
        for logical in sorted(outputs):
            source = outputs[logical]
            if not source.is_file():
                raise RuntimeError(f"CodeGen output is missing or not a file: {source}")
            data = source.read_bytes()
            target = artifact_root.joinpath(*PurePosixPath(logical).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            entries.append((logical, data))

        manifest = _manifest(entries)
        (staging / "SHA256SUMS").write_bytes(manifest)
        (staging / "ROOT_SHA256").write_text(
            _sha256(manifest) + "\n", encoding="ascii", newline="\n"
        )

        if destination.exists():
            if not destination.is_dir():
                raise RuntimeError(
                    f"refusing to replace non-directory destination: {destination}"
                )
            shutil.rmtree(destination)
        os.replace(staging, destination)
        staging = None
    finally:
        if staging is not None and staging.exists():
            shutil.rmtree(staging)


def _self_test() -> None:
    assert (
        _logical_path(
            "bazel-out/k8-opt/bin/tools/minoc/tests/generated/sample.generated.h"
        )
        == "tools/minoc/tests/generated/sample.generated.h"
    )
    assert (
        _logical_path(
            "/tmp/cache/bazel-out/x86_64-fastbuild/bin/mino/schema/fuzz/a.descriptor"
        )
        == "mino/schema/fuzz/a.descriptor"
    )
    try:
        _logical_path("bazel-bin/tools/minoc/output")
    except ValueError:
        pass
    else:
        raise AssertionError("path outside bazel-out configuration was accepted")

    manifest = _manifest((("z", b"last"), ("a", b"first")))
    lines = manifest.decode("ascii").splitlines()
    assert lines[0].endswith("  a")
    assert lines[1].endswith("  z")
    assert len(_sha256(manifest)) == 64
    print("collect_codegen_artifacts.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bazel", default="bazel")
    parser.add_argument("--config", default="release")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--out", type=Path, default=Path("hermetic-codegen"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return 0

    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        parser.error(f"workspace does not contain MODULE.bazel: {workspace}")
    destination = args.out
    if not destination.is_absolute():
        destination = workspace / destination
    outputs = _query_outputs(args.bazel, args.config, workspace)
    _collect(outputs, destination)
    print(
        f"collected {len(outputs)} CodeGen outputs; "
        f"root_sha256={(destination / 'ROOT_SHA256').read_text(encoding='ascii').strip()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
