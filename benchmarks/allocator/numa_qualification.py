#!/usr/bin/env python3
"""Fail-closed qualification runner for the NUMA allocator benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import subprocess
import sys
import time
from typing import Any

SCHEMA = "mino.allocator.numa_qualification_manifest.v1"
BENCHMARK_SCHEMA = "mino.allocator.numa_benchmark.v1"
REQUIRED_MODES = {"local", "interleave", "remote"}


class QualificationError(RuntimeError):
    pass


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def run_text(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise QualificationError(
            f"command failed ({completed.returncode}): {shlex.join(command)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact(path: Path, root: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size == 0:
        raise QualificationError(f"required artifact missing or empty: {path}")
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def governors() -> dict[str, str]:
    result: dict[str, str] = {}
    root = Path("/sys/devices/system/cpu")
    for path in sorted(root.glob("cpu[0-9]*/cpufreq/scaling_governor")):
        value = read_text(path)
        if value:
            result[path.parent.parent.name] = value
    return result


def validate_benchmark(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if report.get("schema") != BENCHMARK_SCHEMA:
        errors.append("benchmark schema mismatch")
    if report.get("status") != "PASSED":
        errors.append(f"benchmark status is {report.get('status')!r}, not PASSED")
    if report.get("qualification_eligible") is not True:
        errors.append("benchmark is not qualification eligible")
    topology = report.get("topology")
    if not isinstance(topology, dict):
        errors.append("topology is missing")
    else:
        if topology.get("linux_native") is not True:
            errors.append("native Linux topology evidence is required")
        if topology.get("numa_available") is not True:
            errors.append("at least two allowed NUMA nodes are required")
        allowed_nodes = topology.get("allowed_nodes")
        if not isinstance(allowed_nodes, list) or len(allowed_nodes) < 2:
            errors.append("allowed_nodes must contain at least two nodes")
    modes = report.get("modes")
    if not isinstance(modes, list):
        errors.append("benchmark modes are missing")
        return errors
    names = {mode.get("name") for mode in modes if isinstance(mode, dict)}
    if names != REQUIRED_MODES or len(modes) != len(REQUIRED_MODES):
        errors.append("local/interleave/remote mode set is incomplete")
    for mode in modes:
        if not isinstance(mode, dict):
            errors.append("malformed mode result")
            continue
        name = mode.get("name", "unknown")
        if not isinstance(mode.get("operations"), int) or mode["operations"] <= 0:
            errors.append(f"{name}: operations must be positive")
        if mode.get("failures") != 0:
            errors.append(f"{name}: allocation failures are non-zero")
        metrics = mode.get("metrics")
        if not isinstance(metrics, dict):
            errors.append(f"{name}: NUMA metrics are missing")
        elif metrics.get("bind_errors") != 0:
            errors.append(f"{name}: bind_errors are non-zero")
        latency = mode.get("latency_ns")
        if not isinstance(latency, dict) or any(
            not isinstance(latency.get(key), int) or latency[key] < 0
            for key in ("p50", "p95", "p99", "p999", "max")
        ):
            errors.append(f"{name}: latency provenance is incomplete")
    provenance = report.get("provenance")
    if not isinstance(provenance, dict):
        errors.append("provenance is missing")
    elif any(provenance.get(key) in (None, "", "PENDING") for key in ("timestamp_utc", "os", "cpu_model")):
        errors.append("provenance contains missing values")
    return errors


def verify_manifest(path: Path, expected_commit: str) -> None:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    errors = []
    if manifest.get("schema") != SCHEMA:
        errors.append("manifest schema mismatch")
    if manifest.get("expected_commit") != expected_commit.lower():
        errors.append("expected_commit mismatch")
    if manifest.get("source_state") != "clean":
        errors.append("source_state is not clean")
    if manifest.get("qualification_eligible") is not True:
        errors.append("manifest is not qualification eligible")
    if manifest.get("artifacts_complete") is not True:
        errors.append("manifest artifacts are incomplete")
    if manifest.get("outcome") != "passed":
        errors.append("manifest outcome is not passed")
    if manifest.get("errors") != []:
        errors.append("manifest contains qualification errors")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        errors.append("manifest must list benchmark JSON and log artifacts")
    if errors:
        raise QualificationError("; ".join(errors))


def run_qualification(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    benchmark_json = output / "benchmark.json"
    benchmark_log = output / "benchmark.log"
    manifest_path = output / "manifest.json"
    errors: list[str] = []
    started = time.time()

    expected_commit = args.expected_commit.lower()
    commit = ""
    source_state = "unknown"
    observed_governors = governors()
    try:
        if platform.system() != "Linux":
            errors.append(f"qualification requires Linux, got {platform.system()}")
        if args.iterations < 1_000_000:
            errors.append("qualification requires at least 1000000 iterations per thread")
        if args.threads <= 0 or args.slots < args.threads:
            errors.append("qualification requires positive threads and slots >= threads")
        if args.native_attestation != "physical-numa":
            errors.append("native attestation must be exactly physical-numa")
        if not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
            errors.append("expected commit must be a full 40-character SHA-1")
        commit = run_text(["git", "rev-parse", "HEAD"], repo).lower()
        status = run_text(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"], repo
        )
        source_state = "clean" if not status else "dirty"
        if commit != expected_commit:
            errors.append(f"HEAD {commit} does not match expected commit")
        if status:
            errors.append("qualification requires a clean tracked and untracked worktree")
        if not observed_governors:
            errors.append("CPU governor provenance is unavailable")
        elif any(value != args.expected_governor for value in observed_governors.values()):
            errors.append(f"all CPU governors must be {args.expected_governor}")
        benchmark = Path(args.benchmark).resolve()
        if not benchmark.is_file() or not os.access(benchmark, os.X_OK):
            errors.append("benchmark binary is missing or not executable")

        if not errors:
            command = [
                str(benchmark),
                f"--threads={args.threads}",
                f"--iterations={args.iterations}",
                f"--slots={args.slots}",
                f"--json={benchmark_json}",
                "--qualification-attestation=physical-numa",
            ]
            completed = subprocess.run(
                command,
                cwd=repo,
                check=False,
                capture_output=True,
                text=True,
                timeout=args.timeout_seconds,
            )
            benchmark_log.write_text(
                "$ " + shlex.join(command) + "\n\nSTDOUT\n" + completed.stdout
                + "\nSTDERR\n" + completed.stderr,
                encoding="utf-8",
            )
            if completed.returncode != 0:
                errors.append(f"benchmark exited {completed.returncode}")
            if not benchmark_json.is_file():
                errors.append("benchmark JSON was not produced")
            else:
                try:
                    report = json.loads(benchmark_json.read_text(encoding="utf-8"))
                    errors.extend(validate_benchmark(report))
                except (OSError, json.JSONDecodeError) as error:
                    errors.append(f"benchmark JSON is invalid: {error}")
    except (OSError, subprocess.SubprocessError, QualificationError) as error:
        errors.append(str(error))

    artifacts: list[dict[str, Any]] = []
    artifacts_complete = False
    if benchmark_json.is_file() and benchmark_log.is_file():
        try:
            artifacts = [
                artifact(benchmark_json, output),
                artifact(benchmark_log, output),
            ]
            artifacts_complete = True
        except QualificationError as error:
            errors.append(str(error))

    passed = not errors and artifacts_complete
    manifest = {
        "schema": SCHEMA,
        "expected_commit": expected_commit,
        "commit": commit,
        "source_state": source_state,
        "native_attestation": args.native_attestation,
        "host": {
            "system": platform.system(),
            "machine": platform.machine(),
            "kernel": platform.release(),
            "cpu_governors": observed_governors,
            "expected_governor": args.expected_governor,
        },
        "qualification_eligible": passed,
        "artifacts_complete": artifacts_complete,
        "outcome": "passed" if passed else "failed",
        "errors": errors,
        "artifacts": artifacts,
        "duration_seconds": round(time.time() - started, 3),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if passed else 1


def self_test() -> int:
    skipped = {
        "schema": BENCHMARK_SCHEMA,
        "status": "SKIPPED",
        "qualification_eligible": False,
        "topology": {"linux_native": True, "numa_available": False, "allowed_nodes": [0]},
        "modes": [],
        "provenance": {"timestamp_utc": "now", "os": "Linux", "cpu_model": "test"},
    }
    errors = validate_benchmark(skipped)
    if not errors or not any("not PASSED" in error for error in errors):
        raise QualificationError("SKIPPED benchmark was not rejected fail-closed")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=".")
    parser.add_argument("--benchmark")
    parser.add_argument("--output-dir")
    parser.add_argument("--expected-commit")
    parser.add_argument("--native-attestation", default="")
    parser.add_argument("--expected-governor", default="performance")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--iterations", type=int, default=1_000_000)
    parser.add_argument("--slots", type=int, default=16_384)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--verify-manifest")
    parser.add_argument("--require-expected-commit")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return args
    if args.verify_manifest:
        if not args.require_expected_commit:
            parser.error("--verify-manifest requires --require-expected-commit")
        return args
    for name in ("benchmark", "output_dir", "expected_commit"):
        if not getattr(args, name):
            parser.error(f"--{name.replace('_', '-')} is required")
    return args


def main() -> int:
    args = parse_args()
    if args.self_test:
        return self_test()
    if args.verify_manifest:
        verify_manifest(Path(args.verify_manifest), args.require_expected_commit)
        return 0
    return run_qualification(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (QualificationError, OSError, json.JSONDecodeError) as error:
        print(f"numa_qualification: {error}", file=sys.stderr)
        sys.exit(2)
