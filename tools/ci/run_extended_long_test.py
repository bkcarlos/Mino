#!/usr/bin/env python3
"""Run one deadline-bounded extended CI suite and emit hashed evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UINT64_MAX = (1 << 64) - 1
MAX_SECONDS = 7200
MARKER = ".mino-extended-long-test"

CommandBuilder = Callable[[str, int], list[str]]


@dataclass(frozen=True)
class Suite:
    description: str
    repeated: bool
    commands: tuple[CommandBuilder, ...]


def _common_test_options(timeout_seconds: int) -> list[str]:
    return [
        "--lockfile_mode=error",
        "--config=gcc12",
        f"--test_timeout={timeout_seconds}",
        "--test_output=streamed",
        "--nocache_test_results",
    ]


def _mpmc_command(bazel: str, remaining_seconds: int) -> list[str]:
    fixed_phase_reserve = min(60, max(1, remaining_seconds // 20))
    timed_phase_seconds = max(0, remaining_seconds - fixed_phase_reserve)
    return [
        bazel,
        "test",
        "--lockfile_mode=error",
        "--config=gcc12",
        "--config=tsan",
        "//mino/shm/channel:mpmc_ring_stress_test",
        f"--test_timeout={remaining_seconds}",
        f"--test_env=MPMC_STRESS_DURATION_SEC={timed_phase_seconds}",
        "--test_env=MPMC_STRESS_TOKENS=65536",
        "--test_output=streamed",
        "--nocache_test_results",
    ]


def _d2_lease_kill_command(bazel: str, remaining_seconds: int) -> list[str]:
    return [
        bazel,
        "test",
        *_common_test_options(remaining_seconds),
        "//mino/runtime:d2_recovery_stress_test",
        "--test_arg=--gtest_filter="
        + "D2RecoveryStressTest.DeadBroadcastSubscriberLeaseClearsRealAcks",
    ]


def _d2_broadcast_kill_command(bazel: str, remaining_seconds: int) -> list[str]:
    test_filter = ":".join(
        [
            "BroadcastChannelXprocTest.CrashedAckCleanupTokenIsRecovered",
            "BroadcastChannelXprocTest.PublisherCrashTombstoneSkipped",
        ]
    )
    return [
        bazel,
        "test",
        *_common_test_options(remaining_seconds),
        "//mino/shm/channel:broadcast_channel_xproc_test",
        f"--test_arg=--gtest_filter={test_filter}",
    ]


def _d4_two_node_command(bazel: str, remaining_seconds: int) -> list[str]:
    return [
        bazel,
        "test",
        *_common_test_options(remaining_seconds),
        "//mino/bridge:bridge_two_node_test",
    ]


def _d4_reconnect_command(bazel: str, remaining_seconds: int) -> list[str]:
    test_filter = ":".join(
        [
            "BridgePipelineTest.*Reconnect*",
            "BridgePipelineTest.ReceiverRestart*",
        ]
    )
    return [
        bazel,
        "test",
        *_common_test_options(remaining_seconds),
        "//mino/bridge:bridge_pipeline_test",
        f"--test_arg=--gtest_filter={test_filter}",
    ]


SUITES = {
    "d1-mpmc-tsan": Suite(
        description="D1 MPMC high-contention stress under ThreadSanitizer",
        repeated=False,
        commands=(_mpmc_command,),
    ),
    "d2-subscriber-broadcast-kill": Suite(
        description="D2 killed subscriber and broadcast recovery scenarios",
        repeated=True,
        commands=(_d2_lease_kill_command, _d2_broadcast_kill_command),
    ),
    "d4-two-node-reconnect": Suite(
        description="D4 real TCP two-node and reconnect/restart soak",
        repeated=True,
        commands=(_d4_two_node_command, _d4_reconnect_command),
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


def _capture(command: list[str], workspace: Path) -> str | None:
    try:
        result = subprocess.run(
            command,
            cwd=workspace,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    output = result.stdout.strip()
    return output if output else None


def _commit(workspace: Path) -> str | None:
    output = _capture(["git", "rev-parse", "HEAD"], workspace)
    if output:
        candidate = output.splitlines()[-1]
        if len(candidate) == 40:
            return candidate
    return os.environ.get("GITHUB_SHA")


def _github_provenance(environment: Mapping[str, str]) -> dict[str, str | None]:
    names = (
        "GITHUB_ACTIONS",
        "GITHUB_EVENT_NAME",
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_REPOSITORY",
        "GITHUB_WORKFLOW",
        "GITHUB_RUN_ATTEMPT",
        "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER",
        "GITHUB_SHA",
        "RUNNER_ARCH",
        "RUNNER_NAME",
        "RUNNER_OS",
    )
    return {name.lower(): environment.get(name) for name in names}


def _prepare_output(output: Path, clean: bool) -> None:
    if output.exists():
        if output.is_symlink() or not output.is_dir():
            raise ValueError(f"output is not a real directory: {output}")
        if any(output.iterdir()):
            if not clean:
                raise ValueError(f"output is not empty: {output}")
            if not (output / MARKER).is_file():
                raise ValueError(f"refusing to clean an unmarked directory: {output}")
            shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / MARKER).write_text("Mino extended long-test evidence\n", encoding="ascii")


def _write_json_atomic(path: Path, value: Any) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump(value, destination, indent=2, sort_keys=True)
            destination.write("\n")
            destination.flush()
            os.fsync(destination.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        pass


def _stream(
    command: list[str],
    workspace: Path,
    log: Path,
    *,
    wall_timeout_seconds: float,
) -> tuple[int, bool]:
    printable = shlex.join(command)
    print(f"+ {printable}", flush=True)
    with log.open("a", encoding="utf-8", newline="\n") as output:
        output.write(f"\n===== {_utc_now()} =====\n+ {printable}\n")
        output.flush()
        try:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                start_new_session=True,
            )
        except OSError as error:
            message = f"unable to start command: {error}\n"
            output.write(message)
            print(message, end="", file=sys.stderr, flush=True)
            return 127, False

        timed_out = threading.Event()

        def expire() -> None:
            timed_out.set()
            _terminate_process_group(process)

        timer = threading.Timer(max(0.0, wall_timeout_seconds), expire)
        timer.daemon = True
        timer.start()
        assert process.stdout is not None
        try:
            for line in process.stdout:
                output.write(line)
                output.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
            return_code = process.wait()
            if timed_out.is_set():
                output.write("extended test exhausted its total time budget\n")
                output.flush()
                return 124, True
            return return_code, False
        except KeyboardInterrupt:
            _terminate_process_group(process)
            process.wait()
            output.write("extended test interrupted\n")
            output.flush()
            return 130, False
        finally:
            timer.cancel()


def _run_suite(
    workspace: Path,
    suite_name: str,
    seconds: int,
    requested_seed: int,
    output: Path,
    *,
    bazel: str,
    clean: bool,
) -> int:
    suite = SUITES[suite_name]
    _prepare_output(output, clean)
    log_path = output / "console.log"
    manifest_path = output / "manifest.json"
    executions: list[dict[str, Any]] = []
    commands: list[list[str]] = []
    started_at = _utc_now()
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "suite": suite_name,
        "description": suite.description,
        "commit": _commit(workspace),
        "seed": None,
        "requested_seed": requested_seed,
        "seed_consumed": False,
        "seed_note": "underlying test targets expose no seed input",
        "command": commands,
        "requested_duration_seconds": seconds,
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "elapsed_seconds": None,
        "outcome": "running",
        "status": "running",
        "exit_code": None,
        "executions": executions,
        "logs": {},
        "github": _github_provenance(os.environ),
    }
    _write_json_atomic(manifest_path, manifest)

    campaign_started = time.monotonic()
    deadline = campaign_started + seconds
    exit_code = 0
    cycle = 0
    stop = seconds == 0
    try:
        while not stop:
            for command_index, builder in enumerate(suite.commands):
                remaining = deadline - time.monotonic()
                remaining_seconds = math.floor(remaining)
                if remaining_seconds < 1:
                    stop = True
                    break
                command = builder(bazel, remaining_seconds)
                wall_timeout = deadline - time.monotonic()
                if wall_timeout <= 0:
                    stop = True
                    break
                commands.append(command)
                command_started_at = _utc_now()
                command_started = time.monotonic()
                result, timed_out = _stream(
                    command,
                    workspace,
                    log_path,
                    wall_timeout_seconds=wall_timeout,
                )
                executions.append(
                    {
                        "cycle": cycle,
                        "command_index": command_index,
                        "command": command,
                        "budget_seconds": remaining_seconds,
                        "started_at_utc": command_started_at,
                        "elapsed_seconds": round(time.monotonic() - command_started, 3),
                        "timed_out": timed_out,
                        "exit_code": result,
                    }
                )
                _write_json_atomic(manifest_path, manifest)
                if result != 0:
                    exit_code = result
                    stop = True
                    break
            if stop or not suite.repeated:
                break
            cycle += 1
    finally:
        elapsed = round(time.monotonic() - campaign_started, 3)
        outcome = "skipped" if not executions else (
            "passed" if exit_code == 0 else "failed"
        )
        manifest.update(
            {
                "finished_at_utc": _utc_now(),
                "elapsed_seconds": elapsed,
                "outcome": outcome,
                "status": outcome,
                "exit_code": exit_code,
                "logs": {
                    "console.log": {
                        "sha256": _sha256(log_path) if log_path.is_file() else None,
                        "size_bytes": log_path.stat().st_size if log_path.is_file() else None,
                    }
                },
            }
        )
        _write_json_atomic(manifest_path, manifest)

    print(f"evidence={output}", flush=True)
    return exit_code


def _seconds(raw: str) -> int:
    try:
        value = int(raw, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("seconds must be a decimal integer") from error
    if not 0 <= value <= MAX_SECONDS:
        raise argparse.ArgumentTypeError(
            f"seconds must be between 0 and {MAX_SECONDS}"
        )
    return value


def _seed(raw: str) -> int:
    try:
        value = int(raw, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("seed must be a decimal integer") from error
    if not 0 <= value <= UINT64_MAX:
        raise argparse.ArgumentTypeError(f"seed must be between 0 and {UINT64_MAX}")
    return value


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "MODULE.bazel").write_text('module(name = "self_test")\n')
        marker = root / "fake-bazel-ran"
        fake_bazel = root / "fake-bazel"
        fake_bazel.write_text(
            "#!/bin/sh\nprintf ran > \"$FAKE_MARKER\"\nsleep 5\n",
            encoding="utf-8",
        )
        fake_bazel.chmod(0o755)
        previous_marker = os.environ.get("FAKE_MARKER")
        os.environ["FAKE_MARKER"] = str(marker)
        try:
            zero_output = root / "zero-evidence"
            exit_code = _run_suite(
                workspace,
                "d4-two-node-reconnect",
                0,
                42,
                zero_output,
                bazel=str(fake_bazel),
                clean=False,
            )
            assert exit_code == 0
            assert not marker.exists()
            zero_manifest = json.loads(
                (zero_output / "manifest.json").read_text(encoding="utf-8")
            )
            assert zero_manifest["outcome"] == "skipped"
            assert zero_manifest["executions"] == []
            assert zero_manifest["seed"] is None
            assert zero_manifest["requested_seed"] == 42
            assert not zero_manifest["seed_consumed"]

            timeout_output = root / "timeout-evidence"
            started = time.monotonic()
            exit_code = _run_suite(
                workspace,
                "d4-two-node-reconnect",
                2,
                43,
                timeout_output,
                bazel=str(fake_bazel),
                clean=False,
            )
            elapsed = time.monotonic() - started
            assert exit_code == 124
            assert elapsed < 2.5
            timeout_manifest = json.loads(
                (timeout_output / "manifest.json").read_text(encoding="utf-8")
            )
            assert timeout_manifest["outcome"] == "failed"
            assert timeout_manifest["executions"][0]["budget_seconds"] == 1
            assert timeout_manifest["executions"][0]["timed_out"]
            assert "--test_timeout=1" in timeout_manifest["command"][0]
        finally:
            if previous_marker is None:
                del os.environ["FAKE_MARKER"]
            else:
                os.environ["FAKE_MARKER"] = previous_marker
        assert _seed(str(UINT64_MAX)) == UINT64_MAX
        assert _seconds("0") == 0
        assert _seconds("3600") == 3600
    print("run_extended_long_test.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", nargs="?", choices=sorted(SUITES))
    parser.add_argument("--seconds", type=_seconds)
    parser.add_argument("--seed", type=_seed)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--out", type=Path)
    parser.add_argument("--bazel", default="bazel")
    parser.add_argument("--clean", action="store_true")
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

    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        parser.error(f"workspace does not contain MODULE.bazel: {workspace}")
    output = args.out
    if not output.is_absolute():
        output = workspace / output
    try:
        return _run_suite(
            workspace,
            args.suite,
            args.seconds,
            args.seed,
            output.resolve(),
            bazel=args.bazel,
            clean=args.clean,
        )
    except ValueError as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
