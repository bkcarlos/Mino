#!/usr/bin/env python3
"""Run a D3 libFuzzer campaign and emit self-contained JSON evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

TARGET = "//mino/schema/fuzz:libfuzzer_driver"
SELECTOR_NAMES = ("IDL", "Descriptor", "CanonicalPayload")
SELECTOR_PATTERN = re.compile(
    r"D3 libFuzzer selectors: IDL=(\d+) Descriptor=(\d+) "
    r"CanonicalPayload=(\d+)"
)
MARKER = ".d3-fuzz-campaign"


class CampaignError(RuntimeError):
    """A campaign failure whose evidence should still be finalized."""


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> str:
    data = _json_bytes(value)
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git_commit(workspace: Path) -> str | None:
    result = _run_capture(["git", "rev-parse", "HEAD"], workspace)
    output = result.get("output")
    if result.get("exit_code") == 0 and isinstance(output, str):
        candidate = output.splitlines()[-1]
        if len(candidate) == 40:
            return candidate
    return os.environ.get("GITHUB_SHA")


def _hash_tree(directory: Path) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    if directory.is_dir():
        for path in sorted(directory.rglob("*")):
            if path.is_symlink():
                raise CampaignError(f"refusing to hash symlink: {path}")
            if path.is_file():
                entries.append(
                    {
                        "path": path.relative_to(directory).as_posix(),
                        "sha256": _sha256_file(path),
                        "size": path.stat().st_size,
                    }
                )
    root_payload = _json_bytes(entries)
    return {
        "algorithm": "sha256",
        "file_count": len(entries),
        "root_sha256": hashlib.sha256(root_payload).hexdigest(),
        "entries": entries,
    }


def _combined_root(named_trees: dict[str, dict[str, Any]]) -> str:
    roots = {
        name: tree["root_sha256"] for name, tree in sorted(named_trees.items())
    }
    return hashlib.sha256(_json_bytes(roots)).hexdigest()


def _run_capture(command: list[str], workspace: Path) -> dict[str, Any]:
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
        return {
            "command": command,
            "exit_code": result.returncode,
            "output": result.stdout.strip(),
        }
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "exit_code": None, "error": str(error)}


def _environment(
    workspace: Path, sanitizer: str, campaign_environment: dict[str, str]
) -> dict[str, Any]:
    github_keys = (
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
    environment_keys = (
        "ASAN_OPTIONS",
        "CC",
        "CXX",
        "LANG",
        "LC_ALL",
        "TZ",
        "UBSAN_OPTIONS",
    )
    return {
        "schema_version": 1,
        "captured_at": _utc_now(),
        "sanitizer": sanitizer,
        "platform": {
            "machine": platform.machine(),
            "node": platform.node(),
            "python": platform.python_version(),
            "release": platform.release(),
            "system": platform.system(),
            "version": platform.version(),
        },
        "environment": {
            key: campaign_environment[key]
            for key in environment_keys
            if key in campaign_environment
        },
        "github": {key: os.environ[key] for key in github_keys if key in os.environ},
        "tools": {
            "bazel": _run_capture(["bazel", "--version"], workspace),
            "compiler": _run_capture(
                [campaign_environment.get("CXX", "clang++"), "--version"],
                workspace,
            ),
            "git": _run_capture(
                ["git", "--no-pager", "rev-parse", "HEAD"], workspace
            ),
        },
    }


def _stream_command(
    command: list[str], workspace: Path, log_path: Path, environment: dict[str, str]
) -> int:
    with log_path.open("w", encoding="utf-8", newline="\n") as log:
        log.write("command: " + " ".join(command) + "\n")
        log.flush()
        try:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as error:
            log.write(f"failed to start command: {error}\n")
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
            raise


def _selector_counts(log_path: Path) -> dict[str, int | None]:
    if not log_path.is_file():
        return {name: None for name in SELECTOR_NAMES}
    matches = SELECTOR_PATTERN.findall(
        log_path.read_text(encoding="utf-8", errors="replace")
    )
    if not matches:
        return {name: None for name in SELECTOR_NAMES}
    return {
        name: int(value) for name, value in zip(SELECTOR_NAMES, matches[-1])
    }


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
    (output / MARKER).write_text("D3 fuzz campaign evidence\n", encoding="ascii")


def _campaign_command(
    bazel: str,
    sanitizer: str,
    corpus: Path,
    artifacts: Path,
    seconds: int,
    seed: int,
) -> list[str]:
    return [
        bazel,
        "run",
        "--lockfile_mode=error",
        f"--config={sanitizer}",
        "--config=fuzz",
        TARGET,
        "--",
        str(corpus),
        f"-max_total_time={seconds}",
        "-max_len=32769",
        "-timeout=10",
        "-rss_limit_mb=4096",
        "-malloc_limit_mb=512",
        "-jobs=1",
        "-workers=1",
        f"-seed={seed}",
        "-print_final_stats=1",
        f"-artifact_prefix={artifacts}{os.sep}",
    ]


def _merge_command(
    bazel: str,
    sanitizer: str,
    minimized: Path,
    corpus: Path,
) -> list[str]:
    return [
        bazel,
        "run",
        "--lockfile_mode=error",
        f"--config={sanitizer}",
        "--config=fuzz",
        TARGET,
        "--",
        "-merge=1",
        "-timeout=10",
        "-rss_limit_mb=4096",
        "-malloc_limit_mb=512",
        str(minimized),
        str(corpus),
    ]


def _default_seed() -> int:
    raw = os.environ.get("GITHUB_RUN_NUMBER", "1")
    try:
        value = int(raw)
    except ValueError:
        return 1
    return value if value > 0 else 1


def _run_campaign(args: argparse.Namespace) -> int:
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

    corpus = output / "corpus"
    minimized = output / "corpus-minimized"
    artifacts = output / "artifacts"
    artifacts.mkdir()
    fuzz_log = output / "fuzz.log"
    prepare_log = output / "prepare.log"
    minimize_log = output / "minimize.log"
    environment_path = output / "environment.json"
    selectors_path = output / "selector-counts.json"
    corpus_hashes_path = output / "corpus-hashes.json"
    artifact_hashes_path = output / "artifact-hashes.json"
    manifest_path = output / "campaign-manifest.json"

    started_at = _utc_now()
    started_monotonic = time.monotonic()
    child_environment = os.environ.copy()
    child_environment.setdefault("CC", "clang")
    child_environment.setdefault("CXX", "clang++")
    if args.sanitizer == "asan":
        child_environment.setdefault(
            "ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1"
        )
    else:
        child_environment.setdefault(
            "UBSAN_OPTIONS", "print_stacktrace=1:halt_on_error=1"
        )
    environment = _environment(workspace, args.sanitizer, child_environment)
    environment_sha = _write_json(environment_path, environment)

    prepare_command = [
        sys.executable,
        str(workspace / "tools/ci/prepare_d3_fuzz_corpus.py"),
        f"--workspace={workspace}",
        f"--out={corpus}",
    ]
    campaign_command = _campaign_command(
        args.bazel,
        args.sanitizer,
        corpus,
        artifacts,
        args.seconds,
        args.seed,
    )
    merge_command = _merge_command(
        args.bazel, args.sanitizer, minimized, corpus
    )
    exit_codes: dict[str, int | None] = {
        "prepare": None,
        "campaign": None,
        "merge": None,
    }
    outcome = "failed"
    failure: str | None = None
    return_code = 1

    try:
        exit_codes["prepare"] = _stream_command(
            prepare_command, workspace, prepare_log, child_environment
        )
        if exit_codes["prepare"] != 0:
            raise CampaignError(
                f"seed corpus preparation failed with exit code {exit_codes['prepare']}"
            )

        exit_codes["campaign"] = _stream_command(
            campaign_command, workspace, fuzz_log, child_environment
        )
        if exit_codes["campaign"] != 0:
            return_code = exit_codes["campaign"] or 1
            raise CampaignError(
                f"libFuzzer campaign failed with exit code {exit_codes['campaign']}"
            )

        counts = _selector_counts(fuzz_log)
        missing = [name for name, count in counts.items() if not count or count < 1]
        if missing:
            raise CampaignError(
                "fuzz campaign did not exercise every selector: " + ", ".join(missing)
            )

        if args.minimize:
            minimized.mkdir()
            exit_codes["merge"] = _stream_command(
                merge_command, workspace, minimize_log, child_environment
            )
            if exit_codes["merge"] != 0:
                raise CampaignError(
                    f"corpus minimization failed with exit code {exit_codes['merge']}"
                )

        outcome = "passed"
        return_code = 0
    except KeyboardInterrupt:
        failure = "campaign interrupted"
        return_code = 130
    except (CampaignError, OSError) as error:
        failure = str(error)
    finally:
        counts = _selector_counts(fuzz_log)
        all_exercised = all(
            count is not None and count > 0 for count in counts.values()
        )
        selectors = {
            "schema_version": 1,
            "all_exercised": all_exercised,
            "counts": counts,
            "source": fuzz_log.name,
        }
        selectors_sha = _write_json(selectors_path, selectors)

        corpus_trees = {
            "corpus": _hash_tree(corpus),
            "corpus_minimized": _hash_tree(minimized),
        }
        corpus_hashes = {
            "schema_version": 1,
            "algorithm": "sha256",
            "combined_root_sha256": _combined_root(corpus_trees),
            "sets": corpus_trees,
        }
        corpus_hashes_sha = _write_json(corpus_hashes_path, corpus_hashes)

        artifact_tree = _hash_tree(artifacts)
        artifact_hashes = {
            "schema_version": 1,
            "algorithm": "sha256",
            "root_sha256": artifact_tree["root_sha256"],
            "file_count": artifact_tree["file_count"],
            "entries": artifact_tree["entries"],
        }
        artifact_hashes_sha = _write_json(artifact_hashes_path, artifact_hashes)

        finished_at = _utc_now()
        elapsed_seconds = round(time.monotonic() - started_monotonic, 3)
        log_evidence = {
            path.name: {
                "sha256": _sha256_file(path) if path.is_file() else None,
                "size_bytes": path.stat().st_size if path.is_file() else None,
            }
            for path in (prepare_log, fuzz_log, minimize_log)
        }
        manifest = {
            "schema_version": 1,
            "commit": _git_commit(workspace),
            "seed": args.seed,
            "seed_consumed": True,
            "command": campaign_command,
            "requested_duration_seconds": args.seconds,
            "elapsed_seconds": elapsed_seconds,
            "outcome": outcome,
            "exit_code": return_code,
            "github": {
                key.lower(): os.environ.get(key)
                for key in (
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
            },
            "campaign": {
                "target": TARGET,
                "sanitizer": args.sanitizer,
                "requested_seconds": args.seconds,
                "seed": args.seed,
                "minimize_corpus": args.minimize,
                "started_at": started_at,
                "finished_at": finished_at,
                "elapsed_seconds": elapsed_seconds,
            },
            "commands": {
                "prepare": prepare_command,
                "campaign": campaign_command,
                "merge": merge_command if args.minimize else None,
            },
            "result": {
                "outcome": outcome,
                "failure": failure,
                "exit_codes": exit_codes,
                "all_selectors_exercised": all_exercised,
            },
            "evidence": {
                "logs": log_evidence,
                "environment": {
                    "path": environment_path.name,
                    "sha256": environment_sha,
                },
                "selector_counts": {
                    "path": selectors_path.name,
                    "sha256": selectors_sha,
                    "counts": counts,
                },
                "corpus_hashes": {
                    "path": corpus_hashes_path.name,
                    "sha256": corpus_hashes_sha,
                    "root_sha256": corpus_hashes["combined_root_sha256"],
                },
                "artifact_hashes": {
                    "path": artifact_hashes_path.name,
                    "sha256": artifact_hashes_sha,
                    "root_sha256": artifact_hashes["root_sha256"],
                },
            },
        }
        _write_json(manifest_path, manifest)
        print(
            f"D3 fuzz campaign {outcome}: sanitizer={args.sanitizer} "
            f"seconds={args.seconds} manifest={manifest_path}"
        )
        if failure:
            print(f"D3 fuzz campaign failure: {failure}", file=sys.stderr)

    return return_code


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        tree = root / "tree"
        tree.mkdir()
        (tree / "b").write_bytes(b"second")
        (tree / "a").write_bytes(b"first")
        hashes = _hash_tree(tree)
        assert hashes["file_count"] == 2
        assert [entry["path"] for entry in hashes["entries"]] == ["a", "b"]
        assert len(hashes["root_sha256"]) == 64

        log = root / "fuzz.log"
        log.write_text(
            "noise\nD3 libFuzzer selectors: IDL=11 Descriptor=22 "
            "CanonicalPayload=33\n",
            encoding="utf-8",
        )
        assert _selector_counts(log) == {
            "IDL": 11,
            "Descriptor": 22,
            "CanonicalPayload": 33,
        }
        assert _selector_counts(root / "missing") == {
            "IDL": None,
            "Descriptor": None,
            "CanonicalPayload": None,
        }
        previous_sha = os.environ.get("GITHUB_SHA")
        os.environ["GITHUB_SHA"] = "a" * 40
        assert _git_commit(root) == "a" * 40
        if previous_sha is None:
            del os.environ["GITHUB_SHA"]
        else:
            os.environ["GITHUB_SHA"] = previous_sha

        output = root / "output"
        _prepare_output(output, clean=False)
        assert (output / MARKER).is_file()
        (output / "old").write_text("old", encoding="ascii")
        _prepare_output(output, clean=True)
        assert not (output / "old").exists()

        payload = {"z": 1, "a": [2, 3]}
        first = _json_bytes(payload)
        second = _json_bytes(payload)
        assert first == second
        assert first.startswith(b'{\n  "a"')
    print("run_d3_fuzz_campaign.py self-test: PASS")


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a sanitizer-backed D3 libFuzzer campaign"
    )
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--out", type=Path, default=Path("d3-fuzz-campaign"))
    parser.add_argument("--bazel", default="bazel")
    parser.add_argument("--sanitizer", choices=("asan", "ubsan"), default="asan")
    parser.add_argument("--seconds", type=_positive_int, default=60)
    parser.add_argument("--seed", type=_positive_int, default=_default_seed())
    parser.add_argument("--minimize", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return 0
    try:
        return _run_campaign(args)
    except CampaignError as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
