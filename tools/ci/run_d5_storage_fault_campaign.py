#!/usr/bin/env python3
"""Run the fail-closed D5 storage SIGKILL recovery scenario matrix."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TARGET = "//mino/storage:storage_fault_test"
MARKER = ".d5-storage-fault-campaign"
MAX_ROUNDS = 1000
UINT64_MAX = (1 << 64) - 1
RESULT_PATTERN = re.compile(
    r"D5_SCENARIO_RESULT\s+scenario=(?P<scenario>[a-z0-9-]+)\s+"
    r"rounds=(?P<rounds>[0-9]+)\s+cases=(?P<cases>[0-9]+)\s+"
    r"seed=(?P<seed>[0-9]+)"
)


@dataclass(frozen=True)
class Scenario:
    name: str
    test: str
    cuts_per_round: int


SCENARIOS = (
    Scenario(
        "record-write",
        "D5StorageFaultTest.SigkillAtRecordWritesRepairsToLastCompleteCommit",
        8,
    ),
    Scenario(
        "record-sync",
        "D5StorageFaultTest.SigkillBeforeAndAfterRecordAndSealSync",
        4,
    ),
    Scenario(
        "schema",
        "D5StorageFaultTest.SigkillAcrossSchemaPersistencePhases",
        8,
    ),
    Scenario(
        "manifest",
        "D5StorageFaultTest.SigkillAcrossManifestPersistencePhases",
        4,
    ),
    Scenario(
        "checkpoint-seal",
        "D5StorageFaultTest.SigkillAcrossSealAndCheckpointPersistence",
        8,
    ),
    Scenario(
        "orphan",
        "D5StorageFaultTest.SigkillAcrossOrphanQuarantinePersistence",
        2,
    ),
)


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
    if result.returncode == 0 and candidate and re.fullmatch(r"[0-9a-fA-F]{40}", candidate[-1]):
        return candidate[-1].lower()
    fallback = os.environ.get("GITHUB_SHA")
    return fallback.lower() if fallback else None


def _git_worktree_changes(workspace: Path) -> list[str] | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=workspace,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return [line for line in result.stdout.splitlines() if line]


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
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


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
            for child in output.iterdir():
                if child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child)
                else:
                    child.unlink()
    output.mkdir(parents=True, exist_ok=True)
    (output / MARKER).write_text("D5 storage fault campaign evidence\n", encoding="ascii")


def _command(
    bazel: str,
    rounds: int,
    seed: int,
    timeout: int,
    scenario: Scenario,
) -> list[str]:
    return [
        bazel,
        "--batch",
        "test",
        "--lockfile_mode=error",
        TARGET,
        f"--test_timeout={timeout}",
        f"--test_env=MINO_D5_STORAGE_FAULT_ROUNDS={rounds}",
        f"--test_env=MINO_D5_STORAGE_FAULT_SEED={seed}",
        "--test_output=streamed",
        "--nocache_test_results",
        f"--test_arg=--gtest_filter={scenario.test}",
    ]


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def _stream(
    command: list[str], workspace: Path, log_path: Path, runner_timeout: int
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
                start_new_session=True,
            )
        except OSError as error:
            log.write(f"unable to start Bazel: {error}\n")
            return 127
        assert process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + runner_timeout
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    message = (
                        f"runner exceeded {runner_timeout}-second process-group timeout\n"
                    )
                    log.write(message)
                    log.flush()
                    print(message, end="", file=sys.stderr, flush=True)
                    _terminate_process_group(process)
                    return 124
                for key, _ in selector.select(timeout=min(0.25, remaining)):
                    line = key.fileobj.readline()
                    if line:
                        log.write(line)
                        log.flush()
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    else:
                        selector.unregister(key.fileobj)
                if process.poll() is not None:
                    tail = process.stdout.read()
                    if tail:
                        log.write(tail)
                        log.flush()
                        sys.stdout.write(tail)
                        sys.stdout.flush()
                    return process.returncode
        except KeyboardInterrupt:
            _terminate_process_group(process)
            return 130
        finally:
            selector.close()


def _reported_results(log_path: Path) -> list[dict[str, int | str]]:
    if not log_path.is_file():
        return []
    results: list[dict[str, int | str]] = []
    text = log_path.read_text(encoding="utf-8", errors="replace")
    for match in RESULT_PATTERN.finditer(text):
        results.append(
            {
                "scenario": match.group("scenario"),
                "rounds": int(match.group("rounds"), 10),
                "cases": int(match.group("cases"), 10),
                "seed": int(match.group("seed"), 10),
            }
        )
    return results


def _scenario_record(
    scenario: Scenario, rounds: int, seed: int, timeout: int, command: list[str]
) -> dict[str, Any]:
    return {
        "name": scenario.name,
        "test": scenario.test,
        "rounds": rounds,
        "seed": seed,
        "cuts_per_round": scenario.cuts_per_round,
        "expected_cases": rounds * scenario.cuts_per_round,
        "reported_cases": None,
        "command": command,
        "timeout_seconds": timeout,
        "started_at": None,
        "finished_at": None,
        "elapsed_seconds": None,
        "outcome": "not_run",
        "exit_code": None,
        "marker_valid": False,
        "reported_markers": [],
        "log": {
            "path": f"{scenario.name}.log",
            "sha256": None,
            "size_bytes": None,
        },
    }


def _run(args: argparse.Namespace) -> int:
    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        raise CampaignError(f"workspace does not contain MODULE.bazel: {workspace}")
    source = workspace / "mino/storage/storage_fault_test.cc"
    build_file = workspace / "mino/storage/BUILD.bazel"
    if not source.is_file() or not build_file.is_file():
        raise CampaignError("storage fault test source or BUILD target is missing")

    output = args.out
    if not output.is_absolute():
        output = workspace / output
    output = output.resolve()
    if output == workspace or workspace not in output.parents:
        raise CampaignError(f"output must be below the workspace: {output}")

    commit = _git_commit(workspace)
    expected_commit = args.expected_commit.lower() if args.expected_commit else None
    if commit is None:
        raise CampaignError("unable to identify the tested git commit")
    if expected_commit and not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        raise CampaignError("expected commit must be a 40-character hexadecimal SHA")
    if expected_commit and commit != expected_commit:
        raise CampaignError(
            f"workspace commit {commit} does not match expected commit {expected_commit}"
        )
    changes = _git_worktree_changes(workspace)
    if expected_commit and changes is None:
        raise CampaignError("unable to verify qualification worktree cleanliness")
    if changes and not args.allow_dirty:
        raise CampaignError(
            "worktree is dirty; pass --allow-dirty for non-qualification development evidence"
        )
    source_state = "unknown" if changes is None else ("dirty" if changes else "clean")
    qualification_eligible = expected_commit is not None and changes == []
    _prepare_output(output, args.clean)

    timeout = args.timeout if args.timeout is not None else 120 + args.rounds * 20
    records = []
    for scenario in SCENARIOS:
        command = _command(args.bazel, args.rounds, args.seed, timeout, scenario)
        records.append(_scenario_record(scenario, args.rounds, args.seed, timeout, command))

    manifest_path = output / "campaign-manifest.json"
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    manifest: dict[str, Any] = {
        "schema_version": 3,
        "commit": commit,
        "expected_commit": expected_commit,
        "source_state": source_state,
        "source_changes": changes,
        "allow_dirty": args.allow_dirty,
        "qualification_eligible": qualification_eligible,
        "seed": args.seed,
        "seed_consumed": True,
        "seed_scope": [scenario.test for scenario in SCENARIOS],
        "rounds": args.rounds,
        "scenario_count": len(SCENARIOS),
        "expected_total_cases": sum(
            args.rounds * scenario.cuts_per_round for scenario in SCENARIOS
        ),
        "reported_total_cases": 0,
        "command": [record["command"] for record in records],
        "requested_duration_seconds": None,
        "test_timeout_seconds_per_scenario": timeout,
        "started_at": started_at,
        "finished_at": None,
        "elapsed_seconds": None,
        "outcome": "running",
        "exit_code": None,
        "missing_or_invalid_scenarios": [scenario.name for scenario in SCENARIOS],
        "github": _github_provenance(os.environ),
        "scenarios": records,
        "evidence": {
            "test_source": {
                "path": str(source.relative_to(workspace)),
                "sha256": _sha256(source),
            },
            "build_file": {
                "path": str(build_file.relative_to(workspace)),
                "sha256": _sha256(build_file),
            },
            "runner": {
                "path": str(Path(__file__).resolve().relative_to(workspace)),
                "sha256": _sha256(Path(__file__).resolve()),
            },
        },
    }
    _write_json(manifest_path, manifest)

    first_failure = 0
    for scenario, record in zip(SCENARIOS, records, strict=True):
        log_path = output / record["log"]["path"]
        record["started_at"] = _utc_now()
        scenario_started = time.monotonic()
        exit_code = _stream(
            record["command"], workspace, log_path, timeout + 60
        )
        record["finished_at"] = _utc_now()
        record["elapsed_seconds"] = round(time.monotonic() - scenario_started, 3)
        record["exit_code"] = exit_code
        markers = _reported_results(log_path)
        record["reported_markers"] = markers
        if len(markers) == 1:
            marker = markers[0]
            record["reported_cases"] = marker["cases"]
            record["marker_valid"] = (
                marker["scenario"] == scenario.name
                and marker["rounds"] == args.rounds
                and marker["cases"] == record["expected_cases"]
                and marker["seed"] == args.seed
            )
        record["log"] = {
            "path": log_path.name,
            "sha256": _sha256(log_path) if log_path.is_file() else None,
            "size_bytes": log_path.stat().st_size if log_path.is_file() else None,
        }
        record["outcome"] = (
            "passed" if exit_code == 0 and record["marker_valid"] else "failed"
        )
        if record["outcome"] != "passed" and first_failure == 0:
            first_failure = exit_code if exit_code != 0 else 1

        valid_records = [item for item in records if item["outcome"] == "passed"]
        manifest["reported_total_cases"] = sum(
            int(item["reported_cases"]) for item in valid_records
        )
        manifest["missing_or_invalid_scenarios"] = [
            item["name"] for item in records if item["outcome"] != "passed"
        ]
        _write_json(manifest_path, manifest)
        if exit_code == 130:
            break

    missing = [record["name"] for record in records if record["outcome"] != "passed"]
    if missing and first_failure == 0:
        first_failure = 1
    manifest["reported_total_cases"] = sum(
        int(record["reported_cases"])
        for record in records
        if record["outcome"] == "passed"
    )
    manifest["missing_or_invalid_scenarios"] = missing
    manifest["finished_at"] = _utc_now()
    manifest["elapsed_seconds"] = round(time.monotonic() - started_monotonic, 3)
    manifest["outcome"] = "passed" if not missing else "failed"
    manifest["exit_code"] = first_failure
    _write_json(manifest_path, manifest)

    print(f"evidence={output}", flush=True)
    if missing:
        print(
            "missing or invalid D5 scenarios: " + ", ".join(missing),
            file=sys.stderr,
            flush=True,
        )
    return first_failure


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


def _commit(raw: str) -> str:
    value = raw.lower()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise argparse.ArgumentTypeError(
            "expected commit must be a 40-character hexadecimal SHA"
        )
    return value


def _default_seed() -> int:
    raw = os.environ.get("GITHUB_RUN_NUMBER", "1")
    try:
        value = int(raw, 10)
    except ValueError:
        return 1
    return value if 0 <= value <= UINT64_MAX else 1


def _self_test() -> None:
    assert len({scenario.name for scenario in SCENARIOS}) == len(SCENARIOS)
    assert len({scenario.test for scenario in SCENARIOS}) == len(SCENARIOS)
    assert sum(scenario.cuts_per_round for scenario in SCENARIOS) == 34
    scenario = SCENARIOS[0]
    command = _command("bazel", 7, 9, 260, scenario)
    assert TARGET in command
    assert command[:3] == ["bazel", "--batch", "test"]
    assert "--test_env=MINO_D5_STORAGE_FAULT_ROUNDS=7" in command
    assert "--test_env=MINO_D5_STORAGE_FAULT_SEED=9" in command
    assert command[-1] == f"--test_arg=--gtest_filter={scenario.test}"
    assert _positive_rounds(str(MAX_ROUNDS)) == MAX_ROUNDS
    assert _seed(str(UINT64_MAX)) == UINT64_MAX
    assert _commit("A" * 40) == "a" * 40
    match = RESULT_PATTERN.search(
        "D5_SCENARIO_RESULT scenario=record-write rounds=7 cases=56 seed=9\n"
    )
    assert match is not None
    assert int(match.group("cases"), 10) == 7 * scenario.cuts_per_round
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
        description="Run the complete D5 storage fork/SIGKILL scenario matrix"
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
    parser.add_argument("--expected-commit", type=_commit)
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="allow dirty local evidence, marked qualification_eligible=false",
    )
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
