#!/usr/bin/env python3
"""Build and run the D5 storage fault campaign without editing BUILD files."""

from __future__ import annotations

import argparse
import datetime as dt
import fnmatch
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

TARGET = "//d5_storage_fault_campaign:storage_fault_test"
ROUNDS_SCOPE = "D5StorageFaultTest.SigkillAtRecordWritesRepairsToLastCompleteCommit"
MARKER = ".d5-storage-fault-campaign"
MAX_ROUNDS = 1000
UINT64_MAX = (1 << 64) - 1


class CampaignError(RuntimeError):
    """A campaign setup or execution error."""


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git_commit(workspace: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=workspace,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return os.environ.get("GITHUB_SHA")
    candidate = result.stdout.strip().splitlines()
    if result.returncode == 0 and candidate and len(candidate[-1]) == 40:
        return candidate[-1]
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


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _prepare_output(output: Path, clean: bool) -> None:
    if output.exists():
        if output.is_symlink() or not output.is_dir():
            raise CampaignError(f"output is not a real directory: {output}")
        if any(output.iterdir()):
            if not clean:
                raise CampaignError(
                    f"output is not empty (pass --clean to replace it): {output}"
                )
            if not (output / MARKER).is_file():
                raise CampaignError(
                    f"refusing to clean an unmarked campaign directory: {output}"
                )
            shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / MARKER).write_text("D5 storage fault campaign evidence\n", encoding="ascii")


def _overlay_build() -> str:
    return """package(default_visibility = [\"//visibility:public\"])

cc_test(
    name = \"storage_fault_test\",
    srcs = [\"storage_fault_test.cc\"],
    deps = [
        \"//mino/common:status\",
        \"//mino/schema:canonical\",
        \"//mino/schema:compiler\",
        \"//mino/schema:layout\",
        \"//mino/schema:registry\",
        \"//mino/schema/codegen:artifact_codec\",
        \"//mino/storage:recorder_buffer_pool\",
        \"//mino/storage:recording_manifest\",
        \"//mino/storage:schema_store\",
        \"//mino/storage:segment_format\",
        \"//mino/storage:segment_recovery\",
        \"//mino/storage:segment_writer\",
        \"@googletest//:gtest_main\",
    ],
)
"""


def _create_overlay(workspace: Path, root: Path) -> None:
    source = workspace / "mino/storage/storage_fault_test.cc"
    if not source.is_file():
        raise CampaignError(f"storage fault test source is missing: {source}")
    package = root / "d5_storage_fault_campaign"
    package.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, package / source.name)
    (package / "BUILD.bazel").write_text(_overlay_build(), encoding="utf-8")


def _filter_selects_rounds_test(gtest_filter: str | None) -> bool:
    if gtest_filter is None:
        return True
    positive_filters = gtest_filter.split("-", 1)[0].split(":")
    return any(
        fnmatch.fnmatchcase(ROUNDS_SCOPE, pattern)
        for pattern in positive_filters
        if pattern
    )


def _command(
    bazel: str,
    overlay: Path,
    rounds: int,
    seed: int,
    timeout: int,
    gtest_filter: str | None,
) -> list[str]:
    command = [
        bazel,
        "test",
        "--lockfile_mode=error",
        f"--package_path=%workspace%{os.pathsep}{overlay}",
        TARGET,
        f"--test_timeout={timeout}",
        f"--test_env=MINO_D5_STORAGE_FAULT_ROUNDS={rounds}",
        f"--test_env=MINO_D5_STORAGE_FAULT_SEED={seed}",
        "--test_output=streamed",
        "--nocache_test_results",
    ]
    if gtest_filter:
        command.append(f"--test_arg=--gtest_filter={gtest_filter}")
    return command


def _stream(
    command: list[str], workspace: Path, log_path: Path
) -> int:
    printable = shlex.join(command)
    print(f"+ {printable}", flush=True)
    with log_path.open("w", encoding="utf-8", newline="\n") as log:
        log.write(f"+ {printable}\n")
        log.flush()
        try:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as error:
            log.write(f"unable to start Bazel: {error}\n")
            return 127
        assert process.stdout is not None
        try:
            for line in process.stdout:
                log.write(line)
                log.flush()
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
            return 130


def _run(args: argparse.Namespace) -> int:
    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        raise CampaignError(f"workspace does not contain MODULE.bazel: {workspace}")

    output = args.out
    if not output.is_absolute():
        output = workspace / output
    output = output.resolve()
    if output == workspace or workspace not in output.parents:
        raise CampaignError(f"output must be below the workspace: {output}")
    _prepare_output(output, args.clean)

    overlay_parent = output if args.keep_overlay else None
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if overlay_parent is None:
        temporary = tempfile.TemporaryDirectory(prefix="mino-d5-storage-fault-")
        overlay = Path(temporary.name) / "repository"
    else:
        overlay = overlay_parent / "overlay"
    _create_overlay(workspace, overlay)

    timeout = args.timeout if args.timeout is not None else 120 + args.rounds * 20
    command = _command(
        args.bazel,
        overlay,
        args.rounds,
        args.seed,
        timeout,
        args.gtest_filter,
    )
    manifest_path = output / "campaign-manifest.json"
    log_path = output / "bazel-console.log"
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    source = workspace / "mino/storage/storage_fault_test.cc"
    seed_consumed = _filter_selects_rounds_test(args.gtest_filter)
    exit_code = 125
    try:
        exit_code = _stream(command, workspace, log_path)
    finally:
        elapsed_seconds = round(time.monotonic() - started_monotonic, 3)
        manifest = {
            "schema_version": 1,
            "commit": _git_commit(workspace),
            "seed": args.seed if seed_consumed else None,
            "requested_seed": args.seed,
            "seed_consumed": seed_consumed,
            "seed_scope": ROUNDS_SCOPE if seed_consumed else None,
            "command": command,
            "requested_duration_seconds": None,
            "test_timeout_seconds": timeout,
            "elapsed_seconds": elapsed_seconds,
            "outcome": "passed" if exit_code == 0 else "failed",
            "exit_code": exit_code,
            "github": _github_provenance(os.environ),
            "campaign": {
                "target": TARGET,
                "rounds": args.rounds,
                "rounds_scope": ROUNDS_SCOPE,
                "seed": args.seed if seed_consumed else None,
                "requested_seed": args.seed,
                "seed_scope": ROUNDS_SCOPE if seed_consumed else None,
                "timeout_seconds": timeout,
                "gtest_filter": args.gtest_filter,
                "started_at": started_at,
                "finished_at": _utc_now(),
                "elapsed_seconds": elapsed_seconds,
            },
            "result": {
                "outcome": "passed" if exit_code == 0 else "failed",
                "exit_code": exit_code,
            },
            "evidence": {
                "console_log": {
                    "path": log_path.name,
                    "sha256": _sha256(log_path) if log_path.is_file() else None,
                    "size_bytes": log_path.stat().st_size if log_path.is_file() else None,
                },
                "test_source": {
                    "path": str(source.relative_to(workspace)),
                    "sha256": _sha256(source),
                },
                "overlay_retained": args.keep_overlay,
            },
        }
        _write_json(manifest_path, manifest)
        if temporary is not None:
            temporary.cleanup()
    print(f"evidence={output}", flush=True)
    return exit_code


def _positive_rounds(raw: str) -> int:
    try:
        value = int(raw, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("rounds must be a decimal integer") from error
    if not 1 <= value <= MAX_ROUNDS:
        raise argparse.ArgumentTypeError(
            f"rounds must be between 1 and {MAX_ROUNDS}"
        )
    return value


def _positive_int(raw: str) -> int:
    try:
        value = int(raw, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a decimal integer") from error
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def _seed(raw: str) -> int:
    try:
        value = int(raw, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError("seed must be a decimal integer") from error
    if not 0 <= value <= UINT64_MAX:
        raise argparse.ArgumentTypeError(f"seed must be between 0 and {UINT64_MAX}")
    return value


def _default_seed() -> int:
    raw = os.environ.get("GITHUB_RUN_NUMBER", "1")
    try:
        value = int(raw, 10)
    except ValueError:
        return 1
    return value if 0 <= value <= UINT64_MAX else 1


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        workspace = root / "workspace"
        source = workspace / "mino/storage/storage_fault_test.cc"
        source.parent.mkdir(parents=True)
        source.write_text("// test\n", encoding="utf-8")
        overlay = root / "overlay"
        _create_overlay(workspace, overlay)
        package = overlay / "d5_storage_fault_campaign"
        assert (package / "BUILD.bazel").read_text(encoding="utf-8") == _overlay_build()
        assert (package / source.name).read_text(encoding="utf-8") == "// test\n"
        command = _command("bazel", overlay, 7, 9, 260, "Suite.Test")
        assert f"--test_env=MINO_D5_STORAGE_FAULT_ROUNDS=7" in command
        assert f"--test_env=MINO_D5_STORAGE_FAULT_SEED=9" in command
        assert command[-1] == "--test_arg=--gtest_filter=Suite.Test"
        assert _positive_rounds(str(MAX_ROUNDS)) == MAX_ROUNDS
        assert _seed(str(UINT64_MAX)) == UINT64_MAX
        assert ROUNDS_SCOPE in _command(
            "bazel", overlay, 7, 9, 260, ROUNDS_SCOPE
        )[-1]
        assert _filter_selects_rounds_test(None)
        assert _filter_selects_rounds_test(ROUNDS_SCOPE)
        assert _filter_selects_rounds_test("D5StorageFaultTest.Sigkill*")
        assert not _filter_selects_rounds_test("OtherSuite.OtherTest")
        assert _git_commit(workspace) == os.environ.get("GITHUB_SHA")
        for invalid in ("0", str(MAX_ROUNDS + 1), "not-a-number"):
            try:
                _positive_rounds(invalid)
            except argparse.ArgumentTypeError:
                pass
            else:
                raise AssertionError(f"invalid rounds accepted: {invalid}")
    print("run_d5_storage_fault_campaign.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the D5 storage kill/ENOSPC/disk-stall campaign"
    )
    parser.add_argument(
        "--workspace", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument(
        "--out", type=Path, default=Path("d5-storage-fault-campaign")
    )
    parser.add_argument("--bazel", default="bazel")
    parser.add_argument("--rounds", type=_positive_rounds, default=1)
    parser.add_argument("--seed", type=_seed, default=_default_seed())
    parser.add_argument("--timeout", type=_positive_int)
    parser.add_argument("--gtest-filter")
    parser.add_argument("--keep-overlay", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return 0
    try:
        return _run(args)
    except CampaignError as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
