#!/usr/bin/env python3
"""Non-qualification AArch64 cross-build and QEMU ABI/atomic smoke."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time
from typing import Any


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], repo: Path, log: Path, timeout: int = 300) -> int:
    completed = subprocess.run(
        command,
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    log.write_text(
        "$ " + shlex.join(command) + "\n\n[stdout]\n" + completed.stdout
        + "\n[stderr]\n" + completed.stderr,
        encoding="utf-8",
    )
    return completed.returncode


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--compiler", default="aarch64-linux-gnu-g++")
    parser.add_argument("--qemu", default="qemu-aarch64")
    args = parser.parse_args(argv)

    repo = Path(__file__).resolve().parents[2]
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    binary = output / "abi_atomic_smoke_aarch64"
    compile_log = output / "cross-build.log"
    qemu_log = output / "qemu-smoke.log"
    manifest_path = output / "manifest.json"
    manifest: dict[str, Any] = {
        "schema": "mino.aarch64.cross_smoke_manifest.v1",
        "qualification_eligible": False,
        "execution": "QEMU user-mode smoke; never performance or qualification evidence",
        "outcome": "failed",
        "artifacts": [],
    }
    try:
        compiler = shutil.which(args.compiler)
        qemu = shutil.which(args.qemu)
        if not compiler or not qemu:
            raise RuntimeError(
                f"required tools not found: compiler={args.compiler}, qemu={args.qemu}"
            )
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
            capture_output=True, text=True
        ).stdout.strip().lower()
        status = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=repo, check=True, capture_output=True, text=True
        ).stdout.strip()
        compile_command = [
            compiler,
            "-std=c++20",
            "-O2",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-static",
            "-I.",
            "tests/aarch64/abi_atomic_smoke_test.cc",
            "-o",
            str(binary),
        ]
        if run(compile_command, repo, compile_log) != 0:
            raise RuntimeError("AArch64 cross-build failed")
        machine = subprocess.run(
            ["readelf", "-h", str(binary)], check=True,
            capture_output=True, text=True
        ).stdout
        if "Machine:" not in machine or "AArch64" not in machine:
            raise RuntimeError("cross-built ELF is not AArch64")
        qemu_command = [qemu, str(binary)]
        if run(qemu_command, repo, qemu_log) != 0:
            raise RuntimeError("QEMU AArch64 smoke failed")
        manifest.update({
            "outcome": "passed",
            "commit": commit,
            "source_state": "clean" if not status else "dirty",
            "commands": {"cross_build": compile_command, "qemu_smoke": qemu_command},
            "elf_machine": "AArch64",
            "artifacts": [
                {"path": binary.name, "bytes": binary.stat().st_size, "sha256": digest(binary)},
                {"path": compile_log.name, "bytes": compile_log.stat().st_size, "sha256": digest(compile_log)},
                {"path": qemu_log.name, "bytes": qemu_log.stat().st_size, "sha256": digest(qemu_log)},
            ],
        })
        return_code = 0
    except (OSError, RuntimeError, subprocess.SubprocessError, subprocess.TimeoutExpired) as error:
        manifest["failure"] = str(error)
        return_code = 1
    manifest["finished_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"AArch64 cross/QEMU smoke {manifest['outcome']}: {manifest_path}")
    if return_code:
        print(manifest.get("failure", "failed"), file=sys.stderr)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
