#!/usr/bin/env python3
"""Run and archive the repository's three D2 TLA+ model checks."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict


@dataclass(frozen=True)
class Model:
    name: str
    module: str
    config: str


class ParsedOutput(TypedDict):
    tlc_version: str | None
    completion_marker_found: bool
    status_statistics: dict[str, int]


class HashedInput(TypedDict):
    path: str
    sha256: str


class ModelEvidence(TypedDict):
    name: str
    model: HashedInput
    config: HashedInput
    command: list[str]
    stdout: str
    stdout_sha256: str
    stderr: str
    stderr_sha256: str
    exit_code: int | None
    timed_out: bool
    timeout_seconds: int | None
    duration_seconds: float
    tlc_version: str | None
    completion_marker_found: bool
    success: bool
    status_statistics: dict[str, int]
    error: str | None


class Manifest(TypedDict):
    schema_version: int
    commit: str | None
    expected_commit: str | None
    source_state: str
    source_changes: list[str] | None
    allow_dirty: bool
    qualification_eligible: bool
    seed: None
    seed_consumed: bool
    command: list[list[str]]
    selected_models: list[str]
    per_model_timeout_seconds: int | None
    total_timeout_seconds: int | None
    elapsed_seconds: float | None
    started_at_utc: str
    completed_at_utc: str | None
    complete: bool
    overall_success: bool
    outcome: str
    exit_code: int | None
    jar: HashedInput
    tlc_version: str | None
    models: list[ModelEvidence]
    logs: dict[str, dict[str, str]]
    github: dict[str, str | None]


class Arguments(argparse.Namespace):
    jar: Path | None = None
    out: Path | None = None
    timeout_seconds: int | None = None
    total_timeout_seconds: int | None = None
    model: list[str] | None = None
    expected_commit: str | None = None
    allow_dirty: bool = False
    self_test: bool = False


MODELS = (
    Model("MpscReservation", "MpscReservation.tla", "MpscReservation.cfg"),
    Model(
        "BroadcastMembership",
        "BroadcastMembership.tla",
        "BroadcastMembership.cfg",
    ),
    Model("LeaseEviction", "LeaseEviction.tla", "LeaseEviction.cfg"),
)

_VERSION_RE = re.compile(r"^TLC2 Version (?P<version>.+)$", re.MULTILINE)
_STATE_RE = re.compile(
    r"(?P<generated>[\d,]+) states? generated, "
    + r"(?P<distinct>[\d,]+) distinct states? found, "
    + r"(?P<left>[\d,]+) states? left on queue\."
)
_DEPTH_RE = re.compile(
    r"The depth of the complete state graph search is (?P<depth>[\d,]+)\."
)
_SUCCESS_RE = re.compile(
    r"^Model checking completed\. No error has been found\.$", re.MULTILINE
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _decoded(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


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
    if result.returncode == 0 and candidate:
        commit = candidate[-1].lower()
        if re.fullmatch(r"[0-9a-f]{40}", commit):
            return commit
    fallback = os.environ.get("GITHUB_SHA")
    if fallback and re.fullmatch(r"[0-9a-fA-F]{40}", fallback):
        return fallback.lower()
    return None


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


def _commit(value: str) -> str:
    normalized = value.lower()
    if not re.fullmatch(r"[0-9a-f]{40}", normalized):
        raise argparse.ArgumentTypeError(
            "expected commit must be a 40-character hexadecimal SHA"
        )
    return normalized


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


def _selected_models(names: list[str] | None) -> tuple[Model, ...]:
    if not names:
        return MODELS
    requested = set(names)
    return tuple(model for model in MODELS if model.name in requested)


def _parse_tlc_output(stdout: bytes, stderr: bytes) -> ParsedOutput:
    text = _decoded(stdout + b"\n" + stderr)
    version_match = _VERSION_RE.search(text)
    state_matches = list(_STATE_RE.finditer(text))
    depth_matches = list(_DEPTH_RE.finditer(text))
    statistics: dict[str, int] = {}
    if state_matches:
        final = state_matches[-1].groupdict()
        statistics.update(
            generated_states=int(final["generated"].replace(",", "")),
            distinct_states=int(final["distinct"].replace(",", "")),
            states_left_on_queue=int(final["left"].replace(",", "")),
        )
    if depth_matches:
        statistics["search_depth"] = int(
            depth_matches[-1].group("depth").replace(",", "")
        )
    return {
        "tlc_version": version_match.group("version").strip()
        if version_match
        else None,
        "completion_marker_found": _SUCCESS_RE.search(text) is not None,
        "status_statistics": statistics,
    }


def _write_manifest(path: Path, manifest: Manifest) -> None:
    temporary = path.with_suffix(".json.tmp")
    _ = temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _ = temporary.replace(path)


def _prepare_output(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(f"refusing symlink output directory: {path}")
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"output path is not a directory: {path}")
        if any(path.iterdir()):
            raise ValueError(f"output directory must be empty: {path}")
    else:
        path.mkdir(parents=True)


def _model_entry(
    *,
    workspace: Path,
    model: Model,
    model_path: Path,
    config_path: Path,
    command: list[str],
    stdout_path: Path,
    stderr_path: Path,
    exit_code: int | None,
    timed_out: bool,
    duration_seconds: float,
    parsed: ParsedOutput,
    error: str | None,
    timeout_seconds: int | None,
) -> ModelEvidence:
    success = (
        exit_code == 0
        and not timed_out
        and parsed["completion_marker_found"]
        and error is None
    )
    entry: ModelEvidence = {
        "name": model.name,
        "model": {
            "path": model_path.relative_to(workspace).as_posix(),
            "sha256": _sha256(model_path),
        },
        "config": {
            "path": config_path.relative_to(workspace).as_posix(),
            "sha256": _sha256(config_path),
        },
        "command": command,
        "stdout": stdout_path.relative_to(stdout_path.parents[1]).as_posix(),
        "stdout_sha256": _sha256(stdout_path),
        "stderr": stderr_path.relative_to(stderr_path.parents[1]).as_posix(),
        "stderr_sha256": _sha256(stderr_path),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "duration_seconds": round(duration_seconds, 3),
        "tlc_version": parsed["tlc_version"],
        "completion_marker_found": parsed["completion_marker_found"],
        "success": success,
        "status_statistics": parsed["status_statistics"],
        "error": error,
    }
    return entry


def _run_model(
    *,
    workspace: Path,
    formal_directory: Path,
    output: Path,
    jar: Path,
    model: Model,
    metadir: Path,
    timeout_seconds: int | None,
) -> ModelEvidence:
    model_path = formal_directory / model.module
    config_path = formal_directory / model.config
    result_directory = output / model.name
    result_directory.mkdir()
    stdout_path = result_directory / "stdout.log"
    stderr_path = result_directory / "stderr.log"
    command = [
        "java",
        "-cp",
        str(jar),
        "tlc2.TLC",
        "-deadlock",
        "-metadir",
        str(metadir),
        "-config",
        model.config,
        model.module,
    ]

    print(f"==> {model.name}", flush=True)
    print("+ " + " ".join(command), flush=True)
    started = time.monotonic()
    exit_code: int | None = None
    timed_out = False
    error: str | None = None
    stdout = b""
    stderr = b""
    try:
        result = subprocess.run(
            command,
            cwd=formal_directory,
            check=False,
            capture_output=True,
            timeout=timeout_seconds,
        )
        exit_code = result.returncode
        stdout = result.stdout
        stderr = result.stderr
    except subprocess.TimeoutExpired as exception:
        timed_out = True
        stdout = exception.stdout or b""
        stderr = exception.stderr or b""
        error = f"TLC exceeded the {timeout_seconds}-second timeout"
        stderr += ("\n" + error + "\n").encode("utf-8")
    except OSError as exception:
        error = f"failed to execute TLC: {exception}"
        stderr = (error + "\n").encode("utf-8")

    duration_seconds = time.monotonic() - started
    _ = stdout_path.write_bytes(stdout)
    _ = stderr_path.write_bytes(stderr)
    parsed = _parse_tlc_output(stdout, stderr)
    entry = _model_entry(
        workspace=workspace,
        model=model,
        model_path=model_path,
        config_path=config_path,
        command=command,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        exit_code=exit_code,
        timed_out=timed_out,
        duration_seconds=duration_seconds,
        parsed=parsed,
        error=error,
        timeout_seconds=timeout_seconds,
    )
    status = "PASS" if entry["success"] else "FAIL"
    print(
        f"{model.name}: {status} (exit={exit_code}, {duration_seconds:.1f}s)",
        flush=True,
    )
    return entry


def _self_test() -> None:
    sample = b"""TLC2 Version 2.18 of 20 July 2023
The depth of the complete state graph search is 42.
12,345 states generated, 6,789 distinct states found, 0 states left on queue.
Model checking completed. No error has been found.
"""
    parsed = _parse_tlc_output(sample, b"")
    assert parsed == {
        "tlc_version": "2.18 of 20 July 2023",
        "completion_marker_found": True,
        "status_statistics": {
            "generated_states": 12345,
            "distinct_states": 6789,
            "states_left_on_queue": 0,
            "search_depth": 42,
        },
    }
    failed = _parse_tlc_output(b"TLC2 Version 2.18\nError: boom\n", b"")
    assert failed["tlc_version"] == "2.18"
    assert not failed["completion_marker_found"]
    assert failed["status_statistics"] == {}
    with tempfile.TemporaryDirectory() as temporary:
        fixture = Path(temporary) / "fixture"
        _ = fixture.write_bytes(b"mino-tla-self-test\n")
        assert _sha256(fixture) == (
            "0bdf3b09224019744988947ee2df17b74959830c90eabf6fb6d36d02699632df"
        )
        previous_sha = os.environ.get("GITHUB_SHA")
        os.environ["GITHUB_SHA"] = "b" * 40
        assert _git_commit(Path(temporary)) == "b" * 40
        if previous_sha is None:
            del os.environ["GITHUB_SHA"]
        else:
            os.environ["GITHUB_SHA"] = previous_sha
    assert [model.name for model in _selected_models(None)] == [
        "MpscReservation",
        "BroadcastMembership",
        "LeaseEviction",
    ]
    assert [model.name for model in _selected_models(["BroadcastMembership"])] == [
        "BroadcastMembership"
    ]
    assert _commit("A" * 40) == "a" * 40
    print("run_tla_validation.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all D2 TLA+ models and archive machine-readable evidence."
    )
    _ = parser.add_argument(
        "--jar", type=Path, help="path to a verified tla2tools.jar"
    )
    _ = parser.add_argument(
        "--out", type=Path, help="new or empty evidence directory"
    )
    _ = parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="optional per-model timeout; timed-out models fail and later models still run",
    )
    _ = parser.add_argument(
        "--total-timeout-seconds",
        type=int,
        default=None,
        help="optional wall-clock limit shared by all selected models",
    )
    _ = parser.add_argument(
        "--model",
        action="append",
        choices=[model.name for model in MODELS],
        help="run only this model; may be repeated (default: all models)",
    )
    _ = parser.add_argument("--expected-commit", type=_commit)
    _ = parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="allow dirty local evidence, marked qualification_eligible=false",
    )
    _ = parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(namespace=Arguments())

    if args.self_test:
        _self_test()
        return 0
    if args.jar is None or args.out is None:
        parser.error("--jar and --out are required unless --self-test is used")
    if args.timeout_seconds is not None and args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than zero")
    if args.total_timeout_seconds is not None and args.total_timeout_seconds <= 0:
        parser.error("--total-timeout-seconds must be greater than zero")

    selected_models = _selected_models(args.model)
    workspace = Path(__file__).resolve().parents[2]
    formal_directory = workspace / "docs" / "formal"
    jar = args.jar.expanduser().resolve()
    if not jar.is_file():
        parser.error(f"tla2tools.jar does not exist or is not a file: {jar}")
    for model in selected_models:
        for path in (formal_directory / model.module, formal_directory / model.config):
            if not path.is_file():
                parser.error(f"required formal input is missing: {path}")

    commit = _git_commit(workspace)
    if args.expected_commit is not None and commit != args.expected_commit:
        parser.error(
            f"workspace commit {commit!r} does not match expected commit "
            f"{args.expected_commit}"
        )
    changes = _git_worktree_changes(workspace)
    if args.expected_commit is not None and changes is None:
        parser.error("unable to verify qualification worktree cleanliness")
    if changes and not args.allow_dirty:
        parser.error(
            "worktree is dirty; pass --allow-dirty for non-qualification development evidence"
        )
    source_state = "unknown" if changes is None else ("dirty" if changes else "clean")
    qualification_eligible = args.expected_commit is not None and changes == []

    output = args.out.expanduser().resolve()
    try:
        _prepare_output(output)
    except ValueError as exception:
        parser.error(str(exception))

    manifest_path = output / "manifest.json"
    started_monotonic = time.monotonic()
    manifest: Manifest = {
        "schema_version": 2,
        "commit": commit,
        "expected_commit": args.expected_commit,
        "source_state": source_state,
        "source_changes": changes,
        "allow_dirty": args.allow_dirty,
        "qualification_eligible": qualification_eligible,
        "seed": None,
        "seed_consumed": False,
        "command": [],
        "selected_models": [model.name for model in selected_models],
        "per_model_timeout_seconds": args.timeout_seconds,
        "total_timeout_seconds": args.total_timeout_seconds,
        "elapsed_seconds": None,
        "started_at_utc": _utc_now(),
        "completed_at_utc": None,
        "complete": False,
        "overall_success": False,
        "outcome": "running",
        "exit_code": None,
        "jar": {"path": str(jar), "sha256": _sha256(jar)},
        "tlc_version": None,
        "models": [],
        "logs": {},
        "github": _github_provenance(os.environ),
    }
    _write_manifest(manifest_path, manifest)

    total_timed_out = False
    with tempfile.TemporaryDirectory(prefix="mino-tla-") as temporary:
        state_root = Path(temporary)
        for model in selected_models:
            effective_timeout = args.timeout_seconds
            if args.total_timeout_seconds is not None:
                remaining = args.total_timeout_seconds - (
                    time.monotonic() - started_monotonic
                )
                if remaining < 1:
                    total_timed_out = True
                    break
                remaining_seconds = int(remaining)
                effective_timeout = (
                    remaining_seconds
                    if effective_timeout is None
                    else min(effective_timeout, remaining_seconds)
                )
            entry = _run_model(
                workspace=workspace,
                formal_directory=formal_directory,
                output=output,
                jar=jar,
                model=model,
                metadir=state_root / model.name,
                timeout_seconds=effective_timeout,
            )
            manifest["models"].append(entry)
            manifest["command"].append(entry["command"])
            manifest["logs"][entry["stdout"]] = {
                "sha256": entry["stdout_sha256"]
            }
            manifest["logs"][entry["stderr"]] = {
                "sha256": entry["stderr_sha256"]
            }
            if manifest["tlc_version"] is None and entry["tlc_version"] is not None:
                manifest["tlc_version"] = entry["tlc_version"]
            _write_manifest(manifest_path, manifest)

    manifest["complete"] = True
    manifest["elapsed_seconds"] = round(time.monotonic() - started_monotonic, 3)
    all_models_ran = len(manifest["models"]) == len(selected_models)
    manifest["overall_success"] = (
        all_models_ran
        and not total_timed_out
        and all(entry["success"] for entry in manifest["models"])
    )
    if manifest["overall_success"]:
        exit_code = 0
    elif total_timed_out or any(
        entry["timed_out"] for entry in manifest["models"]
    ):
        exit_code = 124
    else:
        exit_code = 1
    manifest["outcome"] = "passed" if exit_code == 0 else "failed"
    manifest["exit_code"] = exit_code
    manifest["completed_at_utc"] = _utc_now()
    _write_manifest(manifest_path, manifest)
    print(f"evidence={output}")
    print("TLA+ validation: " + ("PASS" if manifest["overall_success"] else "FAIL"))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
