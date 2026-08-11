#!/usr/bin/env python3
"""Fail-closed D6-17 operations drill runner.

The runner executes checked-in tools and Bazel tests without a shell, applies
per-scenario and suite watchdogs, records expected failures explicitly, cleans
scratch state, and emits a hash-addressed evidence manifest.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping
from typing import Any


SCHEMA_VERSION = 1
MARKER = ".mino-operations-drill"
MODES = {"quick", "qualification"}
REQUIRED_SCENARIOS = {
    "tls-credential-invalid",
    "acl-denied",
    "bridge-disconnect-reconnect",
    "subscriber-lease-expired",
    "schema-mismatch",
    "storage-paused-enospc",
    "exporter-failure",
    "capacity-rejection",
    "upgrade-cutover-interrupted",
}
TOP_LEVEL_KEYS = {
    "schema_version",
    "suite_id",
    "modes",
    "required_scenarios",
    "scenarios",
}
MODE_KEYS = {"total_budget_seconds", "qualification"}
SCENARIO_KEYS = {
    "id",
    "title",
    "runbook_anchor",
    "alert_ids",
    "fault_kind",
    "real_execution",
    "modes",
}
SCENARIO_MODE_KEYS = {"budget_seconds", "steps"}
STEP_KEYS = {
    "name",
    "argv",
    "expected_exit_codes",
    "expected_failure",
    "required_output_regex",
}
TOKEN_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class DrillError(RuntimeError):
    """Configuration, provenance, or execution setup failure."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def write_json_atomic(path: Path, value: object) -> None:
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


def capture(
    command: list[str], workspace: Path, environment: Mapping[str, str], timeout: int = 20
) -> tuple[int | None, str | None]:
    try:
        result = subprocess.run(
            command,
            cwd=workspace,
            env=dict(environment),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return None, None
    return result.returncode, result.stdout.strip() or None


def first_line(value: str | None) -> str | None:
    return value.splitlines()[0] if value else None


def git_provenance(workspace: Path, environment: Mapping[str, str]) -> dict[str, Any]:
    commit_code, commit_output = capture(
        ["git", "rev-parse", "HEAD"], workspace, environment
    )
    commit = None
    if commit_code == 0 and commit_output:
        candidate = commit_output.splitlines()[-1].lower()
        if SHA_PATTERN.fullmatch(candidate):
            commit = candidate

    status_code, status_output = capture(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        workspace,
        environment,
        timeout=30,
    )
    changes = None if status_code != 0 else (status_output.splitlines() if status_output else [])
    return {
        "commit": commit,
        "source_state": (
            "unknown" if changes is None else ("dirty" if changes else "clean")
        ),
        "source_changes": changes,
        "source_changes_sha256": (
            None if changes is None else sha256_text("\n".join(changes) + "\n")
        ),
    }


def ci_provenance(environment: Mapping[str, str]) -> dict[str, str | None]:
    names = (
        "CI",
        "GITHUB_ACTIONS",
        "GITHUB_EVENT_NAME",
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_REPOSITORY",
        "GITHUB_RUN_ATTEMPT",
        "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER",
        "GITHUB_SHA",
        "GITHUB_WORKFLOW",
        "RUNNER_ARCH",
        "RUNNER_NAME",
        "RUNNER_OS",
    )
    return {name.lower(): environment.get(name) for name in names}


def toolchain_provenance(
    workspace: Path, bazel: str, environment: Mapping[str, str]
) -> dict[str, Any]:
    bazel_code, bazel_output = capture([bazel, "--version"], workspace, environment)
    python_code, python_output = capture(
        [sys.executable, "--version"], workspace, environment
    )
    return {
        "bazel": first_line(bazel_output) if bazel_code == 0 else None,
        "python": first_line(python_output) if python_code == 0 else None,
        "python_executable": sys.executable,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
    }


def require_exact_keys(value: Mapping[str, Any], expected: set[str], where: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise DrillError(f"{where} keys differ; missing={missing}, unknown={unknown}")


def require_budget(value: Any, where: str, maximum: int = 86_400) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise DrillError(f"{where} must be an integer in [1, {maximum}]")
    return value


def validate_step(step: Any, where: str) -> None:
    if not isinstance(step, dict):
        raise DrillError(f"{where} must be an object")
    require_exact_keys(step, STEP_KEYS, where)
    if not isinstance(step["name"], str) or not TOKEN_PATTERN.fullmatch(step["name"]):
        raise DrillError(f"{where}.name is not a bounded token")
    argv = step["argv"]
    if not isinstance(argv, list) or not argv or len(argv) > 64:
        raise DrillError(f"{where}.argv must contain 1..64 arguments")
    for argument in argv:
        if (
            not isinstance(argument, str)
            or not argument
            or len(argument) > 4096
            or "\x00" in argument
            or "\n" in argument
        ):
            raise DrillError(f"{where}.argv contains an invalid argument")
    if argv[0] not in {"{bazel}", "{python}"}:
        raise DrillError(f"{where}.argv must directly invoke Bazel or Python")
    exits = step["expected_exit_codes"]
    if (
        not isinstance(exits, list)
        or not exits
        or len(exits) > 8
        or any(isinstance(code, bool) or not isinstance(code, int) or not 0 <= code <= 255 for code in exits)
        or len(set(exits)) != len(exits)
    ):
        raise DrillError(f"{where}.expected_exit_codes is invalid")
    expected_failure = step["expected_failure"]
    if not isinstance(expected_failure, bool):
        raise DrillError(f"{where}.expected_failure must be boolean")
    if expected_failure and 0 in exits:
        raise DrillError(f"{where} expected failure must require a non-zero exit")
    if not expected_failure and exits != [0]:
        raise DrillError(f"{where} normal step must expect only exit zero")
    pattern = step["required_output_regex"]
    if not isinstance(pattern, str) or not pattern or len(pattern) > 1024:
        raise DrillError(f"{where}.required_output_regex is invalid")
    try:
        re.compile(pattern)
    except re.error as error:
        raise DrillError(f"{where}.required_output_regex: {error}") from error
    if expected_failure and not pattern.startswith("DRILL_EXPECTED_FAILURE"):
        raise DrillError(f"{where} expected failure needs an explicit evidence marker")


def load_and_validate_manifest(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DrillError(f"cannot read strict drill manifest {path}: {error}") from error
    if not isinstance(document, dict):
        raise DrillError("drill manifest root must be an object")
    require_exact_keys(document, TOP_LEVEL_KEYS, "manifest")
    if document["schema_version"] != SCHEMA_VERSION:
        raise DrillError(f"unsupported manifest schema_version {document['schema_version']!r}")
    if document["suite_id"] != "mino-d6-17-operations":
        raise DrillError("unexpected suite_id")

    modes = document["modes"]
    if not isinstance(modes, dict) or set(modes) != MODES:
        raise DrillError("manifest modes must be exactly quick and qualification")
    for mode_name, mode in modes.items():
        if not isinstance(mode, dict):
            raise DrillError(f"modes.{mode_name} must be an object")
        require_exact_keys(mode, MODE_KEYS, f"modes.{mode_name}")
        require_budget(mode["total_budget_seconds"], f"modes.{mode_name}.total_budget_seconds")
        if mode["qualification"] is not (mode_name == "qualification"):
            raise DrillError(f"modes.{mode_name}.qualification is inconsistent")

    required = document["required_scenarios"]
    if not isinstance(required, list) or len(required) != len(set(required)):
        raise DrillError("required_scenarios must be a unique list")
    if set(required) != REQUIRED_SCENARIOS:
        raise DrillError(
            "required_scenarios differs from the fail-closed runner contract: "
            f"missing={sorted(REQUIRED_SCENARIOS - set(required))}, "
            f"unknown={sorted(set(required) - REQUIRED_SCENARIOS)}"
        )

    scenarios = document["scenarios"]
    if not isinstance(scenarios, list) or not scenarios:
        raise DrillError("scenarios must be a non-empty list")
    ids: list[str] = []
    for index, scenario in enumerate(scenarios):
        where = f"scenarios[{index}]"
        if not isinstance(scenario, dict):
            raise DrillError(f"{where} must be an object")
        require_exact_keys(scenario, SCENARIO_KEYS, where)
        scenario_id = scenario["id"]
        if not isinstance(scenario_id, str) or not TOKEN_PATTERN.fullmatch(scenario_id):
            raise DrillError(f"{where}.id is not a bounded token")
        ids.append(scenario_id)
        for field in ("title", "real_execution"):
            if not isinstance(scenario[field], str) or not scenario[field].strip():
                raise DrillError(f"{where}.{field} must be non-empty")
        if not isinstance(scenario["runbook_anchor"], str) or not TOKEN_PATTERN.fullmatch(
            scenario["runbook_anchor"]
        ):
            raise DrillError(f"{where}.runbook_anchor is invalid")
        alerts = scenario["alert_ids"]
        if not isinstance(alerts, list) or len(alerts) != len(set(alerts)) or any(
            not isinstance(alert, str) or not re.fullmatch(r"Mino[A-Za-z0-9]+", alert)
            for alert in alerts
        ):
            raise DrillError(f"{where}.alert_ids is invalid")
        if not isinstance(scenario["fault_kind"], str) or not TOKEN_PATTERN.fullmatch(
            scenario["fault_kind"]
        ):
            raise DrillError(f"{where}.fault_kind is invalid")
        scenario_modes = scenario["modes"]
        if not isinstance(scenario_modes, dict) or set(scenario_modes) != MODES:
            raise DrillError(f"{where}.modes must be exactly quick and qualification")
        for mode_name, mode in scenario_modes.items():
            mode_where = f"{where}.modes.{mode_name}"
            if not isinstance(mode, dict):
                raise DrillError(f"{mode_where} must be an object")
            require_exact_keys(mode, SCENARIO_MODE_KEYS, mode_where)
            scenario_budget = require_budget(mode["budget_seconds"], f"{mode_where}.budget_seconds")
            if scenario_budget > document["modes"][mode_name]["total_budget_seconds"]:
                raise DrillError(f"{mode_where} exceeds the suite budget")
            steps = mode["steps"]
            if not isinstance(steps, list) or not steps:
                raise DrillError(f"{mode_where}.steps must be non-empty")
            step_names: list[str] = []
            for step_index, step in enumerate(steps):
                validate_step(step, f"{mode_where}.steps[{step_index}]")
                step_names.append(step["name"])
            if len(step_names) != len(set(step_names)):
                raise DrillError(f"{mode_where} has duplicate step names")
    if len(ids) != len(set(ids)) or set(ids) != set(required):
        raise DrillError("scenario objects must exactly and uniquely match required_scenarios")
    return document


def prepare_output(output: Path, workspace: Path, clean: bool) -> None:
    if output == workspace or workspace not in output.parents:
        raise DrillError(f"output must be below the workspace: {output}")
    if output.is_symlink():
        raise DrillError(f"refusing symlink output directory: {output}")
    if output.exists() and not output.is_dir():
        raise DrillError(f"output is not a directory: {output}")
    if output.exists() and any(output.iterdir()):
        if not clean:
            raise DrillError(f"output is not empty; pass --clean: {output}")
        if not (output / MARKER).is_file():
            raise DrillError(f"refusing to clean unmarked directory: {output}")
        for child in output.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    (output / MARKER).write_text("Mino D6-17 operations drill evidence\n", encoding="ascii")


def expand_command(
    argv: list[str], workspace: Path, bazel: str, scratch: Path
) -> list[str]:
    replacements = {
        "{workspace}": str(workspace),
        "{bazel}": bazel,
        "{python}": sys.executable,
        "{scratch}": str(scratch),
    }
    expanded: list[str] = []
    for argument in argv:
        value = argument
        for token, replacement in replacements.items():
            value = value.replace(token, replacement)
        if "{" in value or "}" in value:
            raise DrillError(f"unknown command placeholder in {argument!r}")
        expanded.append(value)
    return expanded


def terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def stream_command(
    command: list[str],
    workspace: Path,
    environment: Mapping[str, str],
    log_path: Path,
    watchdog_seconds: float,
) -> tuple[int, bool]:
    printable = shlex.join(command)
    print(f"+ {printable}", flush=True)
    timed_out = False
    with log_path.open("w", encoding="utf-8", newline="\n") as log:
        log.write(f"+ {printable}\n")
        log.flush()
        try:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env=dict(environment),
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
            log.write(message)
            print(message, file=sys.stderr, end="", flush=True)
            return 127, False
        assert process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + watchdog_seconds
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    message = f"watchdog exceeded {watchdog_seconds:.3f} seconds\n"
                    log.write(message)
                    log.flush()
                    print(message, file=sys.stderr, end="", flush=True)
                    terminate_process_group(process)
                    return 124, timed_out
                for _key, _mask in selector.select(timeout=min(0.25, remaining)):
                    line = process.stdout.readline()
                    if line:
                        log.write(line)
                        log.flush()
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    else:
                        selector.unregister(process.stdout)
                if process.poll() is not None:
                    tail = process.stdout.read()
                    if tail:
                        log.write(tail)
                        sys.stdout.write(tail)
                        sys.stdout.flush()
                    return process.returncode, timed_out
        except KeyboardInterrupt:
            terminate_process_group(process)
            return 130, False
        finally:
            selector.close()


def evidence_file(path: Path, workspace: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(workspace)),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def initial_step_record(step: Mapping[str, Any], command: list[str], log_name: str) -> dict[str, Any]:
    return {
        "name": step["name"],
        "command": command,
        "expected_exit_codes": step["expected_exit_codes"],
        "expected_failure": step["expected_failure"],
        "required_output_regex": step["required_output_regex"],
        "started_at": None,
        "finished_at": None,
        "elapsed_seconds": None,
        "exit_code": None,
        "watchdog_timeout": False,
        "output_marker_valid": False,
        "outcome": "not_run",
        "log": {"path": log_name, "sha256": None, "size_bytes": None},
    }


def run(args: argparse.Namespace) -> int:
    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        raise DrillError(f"workspace does not contain MODULE.bazel: {workspace}")
    manifest_path = args.manifest
    if not manifest_path.is_absolute():
        manifest_path = workspace / manifest_path
    manifest_path = manifest_path.resolve()
    if workspace not in manifest_path.parents or not manifest_path.is_file():
        raise DrillError(f"manifest must be a real file below the workspace: {manifest_path}")
    definition = load_and_validate_manifest(manifest_path)

    output = args.out
    if not output.is_absolute():
        output = workspace / output
    output = output.resolve()
    prepare_output(output, workspace, args.clean)

    environment = dict(os.environ)
    git = git_provenance(workspace, environment)
    commit = git["commit"]
    if commit is None:
        raise DrillError("unable to identify tested git commit")
    expected_commit = args.expected_commit.lower() if args.expected_commit else None
    if expected_commit is not None and not SHA_PATTERN.fullmatch(expected_commit):
        raise DrillError("--expected-commit must be a 40-character lowercase hexadecimal SHA")
    if args.mode == "qualification":
        if expected_commit is None:
            raise DrillError("qualification requires --expected-commit")
        if commit != expected_commit:
            raise DrillError(f"commit {commit} does not match expected commit {expected_commit}")
        if git["source_state"] != "clean":
            raise DrillError("qualification requires a clean worktree")
        if args.allow_dirty:
            raise DrillError("--allow-dirty is forbidden in qualification mode")
    elif git["source_state"] == "dirty" and not args.allow_dirty:
        raise DrillError("quick mode worktree is dirty; pass --allow-dirty to record development evidence")
    elif git["source_state"] == "unknown":
        raise DrillError("unable to determine worktree cleanliness")

    selected_mode = definition["modes"][args.mode]
    total_budget = selected_mode["total_budget_seconds"]
    runner_started = time.monotonic()
    records: list[dict[str, Any]] = []
    for scenario in definition["scenarios"]:
        mode = scenario["modes"][args.mode]
        scratch = output / "scratch" / scenario["id"]
        commands = [
            expand_command(step["argv"], workspace, args.bazel, scratch)
            for step in mode["steps"]
        ]
        records.append(
            {
                "id": scenario["id"],
                "title": scenario["title"],
                "runbook_anchor": scenario["runbook_anchor"],
                "alert_ids": scenario["alert_ids"],
                "fault_kind": scenario["fault_kind"],
                "real_execution": scenario["real_execution"],
                "budget_seconds": mode["budget_seconds"],
                "started_at": None,
                "finished_at": None,
                "elapsed_seconds": None,
                "outcome": "not_run",
                "cleanup": "not_run",
                "steps": [
                    initial_step_record(
                        step,
                        command,
                        f"logs/{scenario['id']}-{step['name']}.log",
                    )
                    for step, command in zip(mode["steps"], commands, strict=True)
                ],
            }
        )

    evidence_paths = [
        manifest_path,
        workspace / "tools/operations/drill_runner.py",
        workspace / "tools/operations/credential_preflight_drill.py",
        workspace / "docs/operations/runbook.md",
        workspace / "configs/alerts/mino.rules.yml",
    ]
    for path in evidence_paths:
        if not path.is_file():
            raise DrillError(f"required evidence input is missing: {path}")

    started_at = utc_now()
    result_manifest: dict[str, Any] = {
        "schema_version": 1,
        "suite_id": definition["suite_id"],
        "mode": args.mode,
        "qualification_eligible": args.mode == "qualification",
        "commit": commit,
        "expected_commit": expected_commit,
        "source_state": git["source_state"],
        "source_changes": git["source_changes"],
        "source_changes_sha256": git["source_changes_sha256"],
        "allow_dirty": args.allow_dirty,
        "started_at": started_at,
        "finished_at": None,
        "elapsed_seconds": None,
        "total_budget_seconds": total_budget,
        "outcome": "running",
        "exit_code": None,
        "missing_or_failed_scenarios": [record["id"] for record in records],
        "provenance": {
            "ci": ci_provenance(environment),
            "toolchain": toolchain_provenance(workspace, args.bazel, environment),
        },
        "inputs": [evidence_file(path, workspace) for path in evidence_paths],
        "scenarios": records,
    }
    result_path = output / "drill-manifest.json"
    (output / "logs").mkdir(mode=0o700)
    (output / "scratch").mkdir(mode=0o700)
    write_json_atomic(result_path, result_manifest)

    interrupted = False
    for scenario_definition, record in zip(definition["scenarios"], records, strict=True):
        mode = scenario_definition["modes"][args.mode]
        scenario_started = time.monotonic()
        record["started_at"] = utc_now()
        scratch = output / "scratch" / record["id"]
        scratch.mkdir(mode=0o700)
        scenario_environment = dict(environment)
        scenario_environment["MINO_DRILL_MODE"] = args.mode
        scenario_environment["MINO_DRILL_SCENARIO"] = record["id"]
        scenario_environment["MINO_DRILL_SCRATCH"] = str(scratch)
        scenario_ok = True
        try:
            for step_definition, step_record in zip(mode["steps"], record["steps"], strict=True):
                suite_remaining = total_budget - (time.monotonic() - runner_started)
                scenario_remaining = mode["budget_seconds"] - (
                    time.monotonic() - scenario_started
                )
                watchdog = min(suite_remaining, scenario_remaining)
                if watchdog <= 0:
                    step_record["outcome"] = "watchdog_not_started"
                    step_record["watchdog_timeout"] = True
                    scenario_ok = False
                    continue
                step_record["started_at"] = utc_now()
                step_started = time.monotonic()
                log_path = output / step_record["log"]["path"]
                exit_code, timed_out = stream_command(
                    step_record["command"],
                    workspace,
                    scenario_environment,
                    log_path,
                    watchdog,
                )
                step_record["finished_at"] = utc_now()
                step_record["elapsed_seconds"] = round(time.monotonic() - step_started, 3)
                step_record["exit_code"] = exit_code
                step_record["watchdog_timeout"] = timed_out
                log_text = log_path.read_text(encoding="utf-8", errors="replace")
                step_record["output_marker_valid"] = bool(
                    re.search(step_definition["required_output_regex"], log_text)
                )
                step_record["log"] = {
                    "path": step_record["log"]["path"],
                    "sha256": sha256_file(log_path),
                    "size_bytes": log_path.stat().st_size,
                }
                step_ok = (
                    exit_code in step_definition["expected_exit_codes"]
                    and step_record["output_marker_valid"]
                    and not timed_out
                )
                step_record["outcome"] = "passed" if step_ok else "failed"
                scenario_ok = scenario_ok and step_ok
                if exit_code == 130:
                    interrupted = True
                    break
        finally:
            if args.keep_scratch:
                record["cleanup"] = "retained_by_request"
            else:
                try:
                    shutil.rmtree(scratch)
                    record["cleanup"] = "passed"
                except OSError as error:
                    record["cleanup"] = f"failed: {error}"
                    scenario_ok = False
        record["finished_at"] = utc_now()
        record["elapsed_seconds"] = round(time.monotonic() - scenario_started, 3)
        record["outcome"] = "passed" if scenario_ok else "failed"
        result_manifest["missing_or_failed_scenarios"] = [
            item["id"] for item in records if item["outcome"] != "passed"
        ]
        write_json_atomic(result_path, result_manifest)
        if interrupted:
            break

    for record in records:
        if record["outcome"] == "not_run":
            record["outcome"] = "failed"
    failures = [record["id"] for record in records if record["outcome"] != "passed"]
    result_manifest["missing_or_failed_scenarios"] = failures
    result_manifest["finished_at"] = utc_now()
    result_manifest["elapsed_seconds"] = round(time.monotonic() - runner_started, 3)
    result_manifest["outcome"] = "passed" if not failures else "failed"
    result_manifest["exit_code"] = 0 if not failures else (130 if interrupted else 1)
    write_json_atomic(result_path, result_manifest)

    compact_summary = {
        "schema_version": 1,
        "suite_id": definition["suite_id"],
        "mode": args.mode,
        "commit": commit,
        "source_state": git["source_state"],
        "outcome": result_manifest["outcome"],
        "scenario_count": len(records),
        "passed": sum(record["outcome"] == "passed" for record in records),
        "failed": failures,
        "elapsed_seconds": result_manifest["elapsed_seconds"],
        "manifest": {
            "path": result_path.name,
            "sha256": sha256_file(result_path),
        },
        "scenarios": [
            {
                "id": record["id"],
                "outcome": record["outcome"],
                "elapsed_seconds": record["elapsed_seconds"],
                "cleanup": record["cleanup"],
                "logs": [
                    {
                        "path": step["log"]["path"],
                        "sha256": step["log"]["sha256"],
                    }
                    for step in record["steps"]
                ],
            }
            for record in records
        ],
    }
    write_json_atomic(output / "summary.json", compact_summary)
    print(f"evidence={output}", flush=True)
    print(
        f"operations drill {result_manifest['outcome']}: "
        f"{len(records) - len(failures)}/{len(records)} scenarios passed",
        flush=True,
    )
    if failures:
        print("failed scenarios: " + ", ".join(failures), file=sys.stderr)
    return int(result_manifest["exit_code"])


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=sorted(MODES))
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("configs/drills/operations_drills.json"),
    )
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--bazel", default="bazel")
    parser.add_argument("--expected-commit")
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--keep-scratch", action="store_true")
    args = parser.parse_args(argv)
    if args.out is None:
        args.out = Path(f".cache/operations-drill-{args.mode}")
    return args


def main(argv: list[str]) -> int:
    try:
        return run(parse_args(argv))
    except DrillError as error:
        print(f"operations drill: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
