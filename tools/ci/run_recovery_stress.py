#!/usr/bin/env python3
"""Run one reproducible D1/D2 recovery stress test and archive its evidence."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Optional


UINT64_MAX = (1 << 64) - 1
MAX_SECONDS = (1 << 31) - 121


@dataclass(frozen=True)
class StressConfig:
    target: str
    seconds_env: str
    seed_env: str
    gtest_filter: str
    testlog_relative: Path


CONFIGS = {
    "d1": StressConfig(
        target="//mino/shm/region:d1_region_recovery_kill_stress_test",
        seconds_env="MINO_D1_REGION_RECOVERY_STRESS_SECONDS",
        seed_env="MINO_D1_REGION_RECOVERY_STRESS_SEED",
        gtest_filter=(
            "D1RegionRecoveryKillStressTest.RandomizedTimedRecoveryStress:"
            "D1RegionRecoveryKillStressTest.RequiresPosixSharedMemoryAndProcessSignals"
        ),
        testlog_relative=Path(
            "mino/shm/region/d1_region_recovery_kill_stress_test"
        ),
    ),
    "d2": StressConfig(
        target="//mino/runtime:d2_recovery_stress_test",
        seconds_env="MINO_D2_RECOVERY_STRESS_SECONDS",
        seed_env="MINO_D2_RECOVERY_STRESS_SEED",
        gtest_filter=(
            "D2RecoveryStressTest.RandomizedTimedRecoveryStress:"
            "D2RecoveryStressTest.RequiresPosixSharedMemoryAndProcessSignals"
        ),
        testlog_relative=Path("mino/runtime/d2_recovery_stress_test"),
    ),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _capture(
    command: list[str],
    *,
    cwd: Path,
    timeout: int = 15,
    environment: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=None if environment is None else dict(environment),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    output = result.stdout.strip()
    return output if output else None


def _first_line(value: Optional[str]) -> Optional[str]:
    return value.splitlines()[0] if value else None


def _command_version(
    value: Optional[str], *, cwd: Path, environment: Mapping[str, str]
) -> Optional[str]:
    if not value:
        return None
    try:
        command = shlex.split(value)
    except ValueError:
        return None
    if not command:
        return None
    return _first_line(
        _capture(command + ["--version"], cwd=cwd, environment=environment)
    )


def _git_commit(
    workspace: Path, environment: Mapping[str, str]
) -> Optional[str]:
    commit = _capture(
        ["git", "rev-parse", "HEAD"], cwd=workspace, environment=environment
    )
    if commit and len(commit.splitlines()[-1]) == 40:
        return commit.splitlines()[-1]
    return environment.get("GITHUB_SHA")


def _toolchain(
    workspace: Path, bazel: str, environment: Mapping[str, str]
) -> dict[str, object]:
    bazelversion = workspace / ".bazelversion"
    cc = environment.get("CC", "cc")
    cxx = environment.get("CXX", "c++")
    return {
        "bazel": _first_line(
            _capture([bazel, "--version"], cwd=workspace, environment=environment)
        ),
        "bazel_pinned": (
            bazelversion.read_text(encoding="utf-8").strip()
            if bazelversion.is_file()
            else None
        ),
        "cc": cc,
        "cc_version": _command_version(
            cc, cwd=workspace, environment=environment
        ),
        "cxx": cxx,
        "cxx_version": _command_version(
            cxx, cwd=workspace, environment=environment
        ),
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "os": platform.platform(),
        "machine": platform.machine(),
    }


def _ci_provenance(environment: Mapping[str, str]) -> dict[str, Optional[str]]:
    names = (
        "GITHUB_ACTIONS",
        "GITHUB_REPOSITORY",
        "GITHUB_WORKFLOW",
        "GITHUB_EVENT_NAME",
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_SHA",
        "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER",
        "GITHUB_RUN_ATTEMPT",
        "RUNNER_OS",
        "RUNNER_ARCH",
        "RUNNER_NAME",
    )
    return {name.lower(): environment.get(name) for name in names}


def _write_json_atomic(path: Path, value: object) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _prepare_output(output: Path) -> None:
    if output.is_symlink():
        raise RuntimeError(f"refusing symlink output directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    if not output.is_dir():
        raise RuntimeError(f"output path is not a directory: {output}")
    if any(output.iterdir()):
        raise RuntimeError(f"output directory must be empty: {output}")


def _find_bazel_testlogs(
    workspace: Path, bazel: str, environment: Mapping[str, str]
) -> Path:
    reported = _capture(
        [bazel, "info", "bazel-testlogs"],
        cwd=workspace,
        timeout=60,
        environment=environment,
    )
    if reported:
        candidate = Path(reported.splitlines()[-1])
        if candidate.is_absolute():
            return candidate
        return workspace / candidate
    return workspace / "bazel-testlogs"


def _copy_test_evidence(
    testlogs: Path, config: StressConfig, output: Path
) -> tuple[dict[str, object], dict[str, Optional[str]]]:
    records: dict[str, object] = {}
    hashes: dict[str, Optional[str]] = {}
    source_root = testlogs / config.testlog_relative
    for name in ("test.log", "test.xml"):
        source = source_root / name
        destination = output / name
        if source.is_file():
            shutil.copy2(source, destination)
            digest = _sha256(destination)
            records[name] = {
                "artifact": name,
                "sha256": digest,
                "size_bytes": destination.stat().st_size,
                "source": str(source),
            }
            hashes[name] = digest
        else:
            records[name] = {
                "artifact": None,
                "sha256": None,
                "size_bytes": None,
                "source": str(source),
            }
            hashes[name] = None
    return records, hashes


def _stream_command(
    command: list[str], *, cwd: Path, environment: Mapping[str, str], log: Path
) -> int:
    printable = shlex.join(command)
    print(f"+ {printable}", flush=True)
    with log.open("w", encoding="utf-8") as output:
        output.write(f"+ {printable}\n")
        output.flush()
        try:
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=dict(environment),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as error:
            message = f"unable to start Bazel: {error}\n"
            output.write(message)
            print(message, end="", file=sys.stderr, flush=True)
            return 127

        assert process.stdout is not None
        try:
            for line in process.stdout:
                output.write(line)
                output.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
            return process.wait()
        except KeyboardInterrupt:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            output.write("recovery stress interrupted\n")
            output.flush()
            return 130


def _run_stress(
    workspace: Path,
    suite: str,
    seconds: int,
    seed: int,
    output: Path,
    *,
    bazel: str = "bazel",
    environment: Optional[Mapping[str, str]] = None,
) -> int:
    config = CONFIGS[suite]
    process_environment = dict(os.environ if environment is None else environment)
    _prepare_output(output)
    testlogs = _find_bazel_testlogs(workspace, bazel, process_environment)
    timeout_seconds = seconds + 120
    command = [
        bazel,
        "test",
        "--lockfile_mode=error",
        config.target,
        f"--test_timeout={timeout_seconds}",
        f"--test_env={config.seconds_env}={seconds}",
        f"--test_env={config.seed_env}={seed}",
        f"--test_arg=--gtest_filter={config.gtest_filter}",
        "--test_output=streamed",
        "--nocache_test_results",
    ]

    manifest_path = output / "manifest.json"
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    manifest: dict[str, object] = {
        "schema_version": 1,
        "suite": suite,
        "commit": _git_commit(workspace, process_environment),
        "target": config.target,
        "seed": seed,
        "seed_consumed": True,
        "duration_seconds": seconds,
        "requested_duration_seconds": seconds,
        "bazel_test_timeout_seconds": timeout_seconds,
        "start_time_utc": started_at,
        "end_time_utc": None,
        "elapsed_seconds": None,
        "exit_code": None,
        "outcome": "running",
        "status": "running",
        "command": command,
        "toolchain": _toolchain(workspace, bazel, process_environment),
        "ci": _ci_provenance(process_environment),
        "github": _ci_provenance(process_environment),
        "testlogs_root": str(testlogs),
        "test_logs": {},
        "test_log_hashes": {},
    }
    _write_json_atomic(manifest_path, manifest)

    exit_code = 125
    internal_error: Optional[str] = None
    try:
        exit_code = _stream_command(
            command,
            cwd=workspace,
            environment=process_environment,
            log=output / "bazel-console.log",
        )
    except Exception as error:  # Preserve a manifest for unexpected runner errors.
        internal_error = f"{type(error).__name__}: {error}"
        print(internal_error, file=sys.stderr, flush=True)
    finally:
        try:
            records, hashes = _copy_test_evidence(testlogs, config, output)
            console_log = output / "bazel-console.log"
            console_hash = _sha256(console_log) if console_log.is_file() else None
            records["bazel-console.log"] = {
                "artifact": console_log.name if console_log.is_file() else None,
                "sha256": console_hash,
                "size_bytes": (
                    console_log.stat().st_size if console_log.is_file() else None
                ),
                "source": str(console_log),
            }
            hashes["bazel-console.log"] = console_hash
        except Exception as error:  # Keep the primary result even if copying fails.
            records = {}
            hashes = {}
            copy_error = f"{type(error).__name__}: {error}"
            internal_error = (
                f"{internal_error}; evidence copy failed: {copy_error}"
                if internal_error
                else f"evidence copy failed: {copy_error}"
            )
        manifest.update(
            {
                "end_time_utc": _utc_now(),
                "elapsed_seconds": round(time.monotonic() - started_monotonic, 6),
                "exit_code": exit_code,
                "outcome": (
                    "passed" if exit_code == 0 and not internal_error else "failed"
                ),
                "status": (
                    "passed" if exit_code == 0 and not internal_error else "failed"
                ),
                "test_logs": records,
                "test_log_hashes": hashes,
            }
        )
        if internal_error:
            manifest["runner_error"] = internal_error
        _write_json_atomic(manifest_path, manifest)

    print(f"evidence={output}", flush=True)
    return exit_code if not internal_error else (exit_code or 125)


def _positive_seconds(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("seconds must be a decimal integer") from error
    if not 1 <= parsed <= MAX_SECONDS:
        raise argparse.ArgumentTypeError(
            f"seconds must be between 1 and {MAX_SECONDS}"
        )
    return parsed


def _seed(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("seed must be a decimal integer") from error
    if not 0 <= parsed <= UINT64_MAX:
        raise argparse.ArgumentTypeError(f"seed must be between 0 and {UINT64_MAX}")
    return parsed


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "MODULE.bazel").write_text('module(name = "self_test")\n')
        (workspace / ".bazelversion").write_text("self-test\n")
        testlogs = root / "testlogs"
        fake_bazel = root / "fake-bazel"
        fake_bazel.write_text(
            """#!/usr/bin/env python3
import os
from pathlib import Path
import sys

args = sys.argv[1:]
if args == ["--version"]:
    print("bazel self-test")
    raise SystemExit(0)
if args == ["info", "bazel-testlogs"]:
    print(os.environ["FAKE_TESTLOGS"])
    raise SystemExit(0)
if args and args[0] == "test":
    target_arg = next(arg for arg in args[1:] if arg.startswith("//"))
    target = target_arg[2:].replace(":", "/")
    output = Path(os.environ["FAKE_TESTLOGS"]) / target
    output.mkdir(parents=True, exist_ok=True)
    (output / "test.log").write_text("reproducible failure evidence\\n")
    (output / "test.xml").write_text("<testsuites failures=\\\"1\\\"/>\\n")
    print("fake recovery stress failed as requested")
    raise SystemExit(7)
raise SystemExit(2)
""",
            encoding="utf-8",
        )
        fake_bazel.chmod(0o755)
        output = root / "evidence"
        environment = dict(os.environ)
        environment["FAKE_TESTLOGS"] = str(testlogs)
        exit_code = _run_stress(
            workspace,
            "d1",
            2,
            123456789,
            output,
            bazel=str(fake_bazel),
            environment=environment,
        )
        assert exit_code == 7
        manifest = json.loads((output / "manifest.json").read_text())
        assert manifest["status"] == "failed"
        assert manifest["outcome"] == "failed"
        assert manifest["exit_code"] == 7
        assert manifest["seed_consumed"]
        assert manifest["seed"] == 123456789
        assert manifest["duration_seconds"] == 2
        assert manifest["target"] == CONFIGS["d1"].target
        assert manifest["start_time_utc"] and manifest["end_time_utc"]
        for name in ("test.log", "test.xml", "bazel-console.log"):
            assert (output / name).is_file()
            assert manifest["test_log_hashes"][name] == _sha256(output / name)
        assert _positive_seconds("7200") == 7200
        assert _seed(str(UINT64_MAX)) == UINT64_MAX
        for invalid in ("-1", "1;touch /tmp/not-run", str(UINT64_MAX + 1)):
            try:
                _seed(invalid)
            except argparse.ArgumentTypeError:
                pass
            else:
                raise AssertionError(f"invalid seed accepted: {invalid}")
    print("run_recovery_stress.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run and archive a reproducible D1/D2 recovery stress test."
    )
    parser.add_argument("suite", nargs="?", choices=sorted(CONFIGS))
    parser.add_argument("--seconds", type=_positive_seconds)
    parser.add_argument("--seed", type=_seed)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return 0
    missing = [
        name
        for name in ("suite", "seconds", "seed", "out")
        if getattr(args, name) is None
    ]
    if missing:
        parser.error("the following arguments are required: " + ", ".join(missing))

    workspace = Path(__file__).resolve().parents[2]
    if not (workspace / "MODULE.bazel").is_file():
        parser.error(f"cannot locate Mino workspace from {__file__}")
    output = args.out.resolve()
    try:
        return _run_stress(workspace, args.suite, args.seconds, args.seed, output)
    except RuntimeError as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
