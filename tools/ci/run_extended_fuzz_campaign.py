#!/usr/bin/env python3
"""Run and verify bounded Frame/Segment/Handle libFuzzer campaigns."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

SELECTORS: dict[str, dict[str, Any]] = {
    "frame": {
        "target": "//mino/bridge/fuzz:libfuzzer_driver",
        "corpus": "mino/bridge/fuzz/testdata",
        "max_len": 64 << 10,
        "counter_names": ("FrameBody", "Stream", "Control"),
        "counter_pattern": re.compile(
            r"Extended libFuzzer selectors \(frame\): "
            r"FrameBody=(\d+) Stream=(\d+) Control=(\d+)"
        ),
    },
    "segment": {
        "target": "//mino/storage/fuzz:libfuzzer_driver",
        "corpus": "mino/storage/fuzz/testdata",
        "max_len": 64 << 10,
        "counter_names": ("Format", "Scanner"),
        "counter_pattern": re.compile(
            r"Extended libFuzzer selectors \(segment\): "
            r"Format=(\d+) Scanner=(\d+)"
        ),
    },
    "handle": {
        "target": "//mino/shm/region/fuzz:libfuzzer_driver",
        "corpus": "mino/shm/region/fuzz/testdata",
        "max_len": 256,
        "counter_names": ("ResolverBoundary",),
        "counter_pattern": re.compile(
            r"Extended libFuzzer selectors \(handle\): "
            r"ResolverBoundary=(\d+)"
        ),
    },
}
MARKER = ".extended-fuzz-campaign"
MAX_CAMPAIGN_SECONDS = 3600
CAMPAIGN_BUILD_GRACE_SECONDS = 900
MERGE_DEADLINE_SECONDS = 900
REQUIRED_EVIDENCE = (
    "campaign-manifest.json",
    "environment.json",
    "selector-counts.json",
    "corpus-hashes.json",
    "artifact-hashes.json",
    "prepare.log",
    "fuzz.log",
)


class CampaignError(RuntimeError):
    """A fail-closed campaign or evidence validation failure."""


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
                        "size_bytes": path.stat().st_size,
                    }
                )
    root = hashlib.sha256(_json_bytes(entries)).hexdigest()
    return {"algorithm": "sha256", "file_count": len(entries),
            "root_sha256": root, "entries": entries}


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
        return {"command": command, "exit_code": result.returncode,
                "output": result.stdout.strip()}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "exit_code": None, "error": str(error)}


def _git_commit(workspace: Path) -> str | None:
    result = _run_capture(["git", "rev-parse", "HEAD"], workspace)
    output = result.get("output")
    if result.get("exit_code") == 0 and isinstance(output, str):
        candidate = output.splitlines()[-1]
        if re.fullmatch(r"[0-9a-f]{40}", candidate):
            return candidate
    candidate = os.environ.get("GITHUB_SHA")
    return candidate if candidate and re.fullmatch(r"[0-9a-fA-F]{40}", candidate) else None


def _parse_hex(path: Path) -> bytes:
    compact = "".join(path.read_text(encoding="ascii").split())
    try:
        return bytes.fromhex(compact)
    except ValueError as error:
        raise CampaignError(f"invalid hexadecimal corpus {path}: {error}") from error


def _prepare_corpus(workspace: Path, selector: str, output: Path) -> dict[str, Any]:
    config = SELECTORS[selector]
    source = workspace / str(config["corpus"])
    paths = sorted(source.glob("*.hex"))
    if not paths:
        raise CampaignError(f"no committed corpus files for selector {selector}")
    output.mkdir(parents=True)
    entries: list[dict[str, Any]] = []
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise CampaignError(f"invalid corpus source: {path}")
        data = _parse_hex(path)
        if not data:
            raise CampaignError(f"empty corpus source: {path}")
        if len(data) > int(config["max_len"]):
            raise CampaignError(f"corpus exceeds max_len: {path}")
        digest = hashlib.sha256(data).hexdigest()
        destination = output / digest
        if destination.exists() and destination.read_bytes() != data:
            raise CampaignError(f"corpus digest collision: {destination}")
        destination.write_bytes(data)
        entries.append({"source": path.relative_to(workspace).as_posix(),
                        "sha256": digest, "size_bytes": len(data)})
    return {"selector": selector, "count": len(entries), "entries": entries}


def _prepare_output(output: Path, selector: str, clean: bool) -> None:
    if output.exists():
        if output.is_symlink() or not output.is_dir():
            raise CampaignError(f"output is not a real directory: {output}")
        if any(output.iterdir()):
            if not clean:
                raise CampaignError(f"output is not empty: {output}")
            marker = output / MARKER
            if not marker.is_file() or marker.read_text(encoding="ascii").strip() != selector:
                raise CampaignError(f"refusing to clean unmarked output: {output}")
            shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / MARKER).write_text(selector + "\n", encoding="ascii")


def _stream_command(command: list[str], workspace: Path, log_path: Path,
                    environment: dict[str, str], deadline_seconds: int) -> int:
    with log_path.open("w", encoding="utf-8", newline="\n") as log:
        log.write("command: " + " ".join(command) + "\n")
        log.write(f"deadline_seconds: {deadline_seconds}\n")
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
                start_new_session=True,
            )
        except OSError as error:
            log.write(f"failed to start command: {error}\n")
            return 127

        timed_out = threading.Event()

        def expire() -> None:
            if process.poll() is not None:
                return
            timed_out.set()
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            except OSError:
                process.terminate()
            time.sleep(5)
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError:
                    process.kill()

        timer = threading.Timer(deadline_seconds, expire)
        timer.daemon = True
        timer.start()
        try:
            assert process.stdout is not None
            for line in process.stdout:
                log.write(line)
                log.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
            return_code = process.wait()
        except KeyboardInterrupt:
            expire()
            process.wait()
            raise
        finally:
            timer.cancel()
        if timed_out.is_set():
            log.write("subprocess deadline exceeded\n")
            log.flush()
            return 124
        return return_code


def _selector_counts(selector: str, log_path: Path) -> dict[str, int | None]:
    names = SELECTORS[selector]["counter_names"]
    if not log_path.is_file():
        return {name: None for name in names}
    text = log_path.read_text(encoding="utf-8", errors="replace")
    matches = SELECTORS[selector]["counter_pattern"].findall(text)
    if not matches:
        return {name: None for name in names}
    values = matches[-1]
    if isinstance(values, str):
        values = (values,)
    return {name: int(value) for name, value in zip(names, values)}


def _environment(workspace: Path, selector: str, sanitizer: str,
                 child_environment: dict[str, str]) -> dict[str, Any]:
    keys = ("ASAN_OPTIONS", "CC", "CXX", "LANG", "LC_ALL", "TZ",
            "UBSAN_OPTIONS")
    github_keys = (
        "GITHUB_ACTIONS", "GITHUB_EVENT_NAME", "GITHUB_JOB", "GITHUB_REF",
        "GITHUB_REPOSITORY", "GITHUB_RUN_ATTEMPT", "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER", "GITHUB_SHA", "GITHUB_WORKFLOW", "RUNNER_ARCH",
        "RUNNER_NAME", "RUNNER_OS",
    )
    return {
        "schema_version": 1,
        "captured_at": _utc_now(),
        "selector": selector,
        "sanitizer": sanitizer,
        "platform": {"machine": platform.machine(), "python": platform.python_version(),
                     "release": platform.release(), "system": platform.system()},
        "environment": {key: child_environment[key] for key in keys
                        if key in child_environment},
        "github": {key.lower(): os.environ.get(key) for key in github_keys},
        "tools": {
            "bazel": _run_capture(["bazel", "--version"], workspace),
            "compiler": _run_capture(
                [child_environment.get("CXX", "clang++"), "--version"], workspace),
            "git": _run_capture(["git", "rev-parse", "HEAD"], workspace),
        },
    }


def _campaign_command(args: argparse.Namespace, corpus: Path,
                      artifacts: Path) -> list[str]:
    config = SELECTORS[args.selector]
    return [
        args.bazel, "run", "--lockfile_mode=error",
        f"--config={args.sanitizer}", "--config=fuzz", str(config["target"]),
        "--", str(corpus), f"-max_total_time={args.seconds}",
        f"-max_len={config['max_len']}", "-timeout=10", "-rss_limit_mb=2048",
        "-malloc_limit_mb=256", "-jobs=1", "-workers=1",
        f"-seed={args.seed}", "-print_final_stats=1",
        f"-artifact_prefix={artifacts}{os.sep}",
    ]


def _merge_command(args: argparse.Namespace, minimized: Path,
                   corpus: Path) -> list[str]:
    config = SELECTORS[args.selector]
    return [
        args.bazel, "run", "--lockfile_mode=error",
        f"--config={args.sanitizer}", "--config=fuzz", str(config["target"]),
        "--", "-merge=1", f"-max_len={config['max_len']}", "-timeout=10",
        "-rss_limit_mb=2048", "-malloc_limit_mb=256", str(minimized), str(corpus),
    ]


def _run_campaign(args: argparse.Namespace) -> int:
    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        raise CampaignError(f"workspace does not contain MODULE.bazel: {workspace}")
    output = args.out if args.out.is_absolute() else workspace / args.out
    output = output.resolve()
    if output == workspace or workspace not in output.parents:
        raise CampaignError(f"output must be below workspace: {output}")
    _prepare_output(output, args.selector, args.clean)

    corpus = output / "corpus"
    minimized = output / "corpus-minimized"
    artifacts = output / "artifacts"
    artifacts.mkdir()
    prepare_log = output / "prepare.log"
    fuzz_log = output / "fuzz.log"
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
            "ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1:abort_on_error=1")
    else:
        child_environment.setdefault(
            "UBSAN_OPTIONS", "print_stacktrace=1:halt_on_error=1")
    environment_sha = _write_json(
        environment_path,
        _environment(workspace, args.selector, args.sanitizer, child_environment),
    )
    campaign_command = _campaign_command(args, corpus, artifacts)
    merge_command = _merge_command(args, minimized, corpus)
    exit_codes: dict[str, int | None] = {"prepare": None, "campaign": None,
                                                "merge": None}
    outcome = "failed"
    failure: str | None = None
    return_code = 1

    try:
        prepared = _prepare_corpus(workspace, args.selector, corpus)
        prepare_log.write_text(json.dumps(prepared, indent=2, sort_keys=True) + "\n",
                               encoding="utf-8")
        exit_codes["prepare"] = 0
        exit_codes["campaign"] = _stream_command(
            campaign_command,
            workspace,
            fuzz_log,
            child_environment,
            args.seconds + CAMPAIGN_BUILD_GRACE_SECONDS,
        )
        if exit_codes["campaign"] != 0:
            return_code = exit_codes["campaign"] or 1
            raise CampaignError(
                f"libFuzzer exited with code {exit_codes['campaign']}")
        counts = _selector_counts(args.selector, fuzz_log)
        missing = [name for name, count in counts.items() if not count or count < 1]
        if missing:
            raise CampaignError("unexercised internal selectors: " + ", ".join(missing))
        if args.minimize:
            minimized.mkdir()
            exit_codes["merge"] = _stream_command(
                merge_command,
                workspace,
                minimize_log,
                child_environment,
                MERGE_DEADLINE_SECONDS,
            )
            if exit_codes["merge"] != 0:
                raise CampaignError(
                    f"corpus merge exited with code {exit_codes['merge']}")
        outcome = "passed"
        return_code = 0
    except KeyboardInterrupt:
        failure = "campaign interrupted"
        return_code = 130
    except (CampaignError, OSError) as error:
        failure = str(error)
    finally:
        counts = _selector_counts(args.selector, fuzz_log)
        all_exercised = all(count is not None and count > 0
                            for count in counts.values())
        selectors_sha = _write_json(selectors_path, {
            "schema_version": 1, "selector": args.selector,
            "all_exercised": all_exercised, "counts": counts,
            "source": fuzz_log.name,
        })
        corpus_sets = {"corpus": _hash_tree(corpus),
                       "corpus_minimized": _hash_tree(minimized)}
        corpus_hashes = {
            "schema_version": 1,
            "sets": corpus_sets,
            "combined_root_sha256": hashlib.sha256(
                _json_bytes({name: tree["root_sha256"]
                             for name, tree in sorted(corpus_sets.items())})
            ).hexdigest(),
        }
        corpus_sha = _write_json(corpus_hashes_path, corpus_hashes)
        artifact_hashes = _hash_tree(artifacts)
        artifact_sha = _write_json(artifact_hashes_path, artifact_hashes)
        logs = {
            path.name: {
                "sha256": _sha256_file(path) if path.is_file() else None,
                "size_bytes": path.stat().st_size if path.is_file() else None,
            }
            for path in (prepare_log, fuzz_log, minimize_log)
        }
        elapsed = round(time.monotonic() - started_monotonic, 3)
        manifest = {
            "schema_version": 1,
            "suite": "d0-d5-extended-fuzz",
            "commit": _git_commit(workspace),
            "seed": args.seed,
            "seed_consumed": True,
            "selector": args.selector,
            "sanitizer": args.sanitizer,
            "target": SELECTORS[args.selector]["target"],
            "command": campaign_command,
            "requested_duration_seconds": args.seconds,
            "elapsed_seconds": elapsed,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "outcome": outcome,
            "exit_code": return_code,
            "failure": failure,
            "step_exit_codes": exit_codes,
            "selector_counts": counts,
            "all_internal_selectors_exercised": all_exercised,
            "logs": logs,
            "evidence": {
                "environment": {"path": environment_path.name,
                                "sha256": environment_sha},
                "selector_counts": {"path": selectors_path.name,
                                    "sha256": selectors_sha},
                "corpus_hashes": {"path": corpus_hashes_path.name,
                                  "sha256": corpus_sha,
                                  "root_sha256": corpus_hashes["combined_root_sha256"]},
                "artifact_hashes": {"path": artifact_hashes_path.name,
                                    "sha256": artifact_sha,
                                    "root_sha256": artifact_hashes["root_sha256"]},
            },
        }
        _write_json(manifest_path, manifest)
        print(f"extended fuzz {outcome}: selector={args.selector} "
              f"sanitizer={args.sanitizer} manifest={manifest_path}")
        if failure:
            print(f"extended fuzz failure: {failure}", file=sys.stderr)
    return return_code


def _verify_evidence(
    output: Path,
    expected_commit: str | None,
    expected_seed: int | None,
    expected_selector: str | None,
    expected_sanitizer: str | None,
    expected_seconds: int | None,
) -> None:
    output = output.resolve()
    if output.is_symlink() or not output.is_dir():
        raise CampaignError(f"evidence directory is missing: {output}")
    missing = [name for name in REQUIRED_EVIDENCE
               if not (output / name).is_file()]
    if missing:
        raise CampaignError("required fuzz evidence is missing: " + ", ".join(missing))
    manifest = json.loads((output / "campaign-manifest.json").read_text(
        encoding="utf-8"))
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("suite") != "d0-d5-extended-fuzz"
        or manifest.get("outcome") != "passed"
        or manifest.get("exit_code") != 0
    ):
        raise CampaignError("campaign manifest does not record a passing run")
    selector = manifest.get("selector")
    sanitizer = manifest.get("sanitizer")
    if selector not in SELECTORS:
        raise CampaignError("campaign manifest has an invalid selector")
    if sanitizer not in ("asan", "ubsan"):
        raise CampaignError("campaign manifest has an invalid sanitizer")
    if expected_selector is not None and selector != expected_selector:
        raise CampaignError("campaign selector does not match requested selector")
    if expected_sanitizer is not None and sanitizer != expected_sanitizer:
        raise CampaignError("campaign sanitizer does not match requested sanitizer")
    if expected_commit is not None and manifest.get("commit") != expected_commit:
        raise CampaignError("campaign commit does not match checked-out commit")
    seed = manifest.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool) or seed <= 0:
        raise CampaignError("campaign seed is absent or invalid")
    if expected_seed is not None and seed != expected_seed:
        raise CampaignError("campaign seed does not match requested seed")
    requested_seconds = manifest.get("requested_duration_seconds")
    if (
        not isinstance(requested_seconds, int)
        or isinstance(requested_seconds, bool)
        or not 1 <= requested_seconds <= MAX_CAMPAIGN_SECONDS
    ):
        raise CampaignError("campaign duration is absent or out of bounds")
    if expected_seconds is not None and requested_seconds != expected_seconds:
        raise CampaignError("campaign duration does not match requested duration")
    if manifest.get("target") != SELECTORS[selector]["target"]:
        raise CampaignError("campaign target does not match selector")
    if manifest.get("seed_consumed") is not True:
        raise CampaignError("campaign manifest does not attest seed consumption")
    step_exit_codes = manifest.get("step_exit_codes")
    if (
        not isinstance(step_exit_codes, dict)
        or step_exit_codes.get("prepare") != 0
        or step_exit_codes.get("campaign") != 0
        or step_exit_codes.get("merge") not in (None, 0)
    ):
        raise CampaignError("campaign step exit codes are inconsistent")
    command = manifest.get("command")
    expected_command_tail = [
        "run", "--lockfile_mode=error", f"--config={sanitizer}",
        "--config=fuzz", str(SELECTORS[selector]["target"]), "--",
        str(output / "corpus"), f"-max_total_time={requested_seconds}",
        f"-max_len={SELECTORS[selector]['max_len']}", "-timeout=10",
        "-rss_limit_mb=2048", "-malloc_limit_mb=256", "-jobs=1",
        "-workers=1", f"-seed={seed}",
        "-print_final_stats=1", f"-artifact_prefix={output / 'artifacts'}{os.sep}",
    ]
    if not isinstance(command, list) or command[1:] != expected_command_tail:
        raise CampaignError("campaign command does not match manifest fields")
    expected_count_names = set(SELECTORS[selector]["counter_names"])
    counts = manifest.get("selector_counts")
    if (
        not isinstance(counts, dict)
        or set(counts) != expected_count_names
        or not all(
            isinstance(value, int) and not isinstance(value, bool) and value > 0
            for value in counts.values()
        )
    ):
        raise CampaignError("selector counts are absent or incomplete")
    if not manifest.get("all_internal_selectors_exercised"):
        raise CampaignError("manifest does not attest all internal selectors")
    for log_name in ("prepare.log", "fuzz.log"):
        evidence = manifest.get("logs", {}).get(log_name, {})
        path = output / log_name
        if evidence.get("sha256") != _sha256_file(path):
            raise CampaignError(f"log hash mismatch: {log_name}")
        if evidence.get("size_bytes") != path.stat().st_size:
            raise CampaignError(f"log size mismatch: {log_name}")
    for key in ("environment", "selector_counts", "corpus_hashes",
                "artifact_hashes"):
        evidence = manifest.get("evidence", {}).get(key, {})
        path_value = evidence.get("path")
        if not isinstance(path_value, str) or Path(path_value).name != path_value:
            raise CampaignError(f"invalid evidence path for {key}")
        path = output / path_value
        if not path.is_file() or evidence.get("sha256") != _sha256_file(path):
            raise CampaignError(f"evidence hash mismatch: {key}")

    environment = json.loads((output / "environment.json").read_text(
        encoding="utf-8"))
    if (
        not isinstance(environment, dict)
        or environment.get("schema_version") != 1
        or environment.get("selector") != selector
        or environment.get("sanitizer") != sanitizer
    ):
        raise CampaignError("environment evidence differs from campaign manifest")

    selector_counts = json.loads((output / "selector-counts.json").read_text(
        encoding="utf-8"))
    if (
        not isinstance(selector_counts, dict)
        or selector_counts.get("schema_version") != 1
        or selector_counts.get("selector") != selector
        or selector_counts.get("counts") != counts
        or selector_counts.get("all_exercised") is not True
        or selector_counts.get("source") != "fuzz.log"
    ):
        raise CampaignError("selector-count evidence differs from campaign manifest")

    corpus_hashes = json.loads((output / "corpus-hashes.json").read_text(
        encoding="utf-8"))
    if not isinstance(corpus_hashes, dict) or set(
        corpus_hashes.get("sets", {})
    ) != {"corpus", "corpus_minimized"}:
        raise CampaignError("corpus hash evidence has an invalid set list")
    for name in ("corpus", "corpus_minimized"):
        recorded = corpus_hashes.get("sets", {}).get(name)
        if not isinstance(recorded, dict):
            raise CampaignError(f"missing recorded corpus set: {name}")
        actual = _hash_tree(output / name)
        if actual != recorded:
            raise CampaignError(f"corpus tree hash mismatch: {name}")
    if corpus_hashes["sets"]["corpus"].get("file_count", 0) < 1:
        raise CampaignError("prepared binary corpus is missing")
    combined_root = hashlib.sha256(
        _json_bytes({
            name: corpus_hashes["sets"][name]["root_sha256"]
            for name in sorted(corpus_hashes["sets"])
        })
    ).hexdigest()
    manifest_corpus_root = manifest.get("evidence", {}).get(
        "corpus_hashes", {}
    ).get("root_sha256")
    if (
        corpus_hashes.get("combined_root_sha256") != combined_root
        or manifest_corpus_root != combined_root
    ):
        raise CampaignError("corpus root hash differs from campaign manifest")

    artifacts = output / "artifacts"
    if artifacts.is_symlink() or not artifacts.is_dir():
        raise CampaignError("libFuzzer artifact directory is missing")
    artifact_hashes = json.loads((output / "artifact-hashes.json").read_text(
        encoding="utf-8"))
    if not isinstance(artifact_hashes, dict):
        raise CampaignError("artifact hash evidence is not an object")
    if _hash_tree(artifacts) != artifact_hashes:
        raise CampaignError("libFuzzer artifact tree hash mismatch")
    manifest_artifact_root = manifest.get("evidence", {}).get(
        "artifact_hashes", {}
    ).get("root_sha256")
    if manifest_artifact_root != artifact_hashes.get("root_sha256"):
        raise CampaignError("artifact root hash differs from campaign manifest")
    print(f"extended fuzz evidence verification: PASS ({output})")


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        corpus = root / "seed.hex"
        corpus.write_text("00 ff\n7F", encoding="ascii")
        assert _parse_hex(corpus) == b"\x00\xff\x7f"
        tree = root / "tree"
        tree.mkdir()
        (tree / "b").write_bytes(b"second")
        (tree / "a").write_bytes(b"first")
        assert [entry["path"] for entry in _hash_tree(tree)["entries"]] == ["a", "b"]
        log = root / "fuzz.log"
        log.write_text(
            "Extended libFuzzer selectors (frame): FrameBody=1 Stream=2 Control=3\n",
            encoding="utf-8",
        )
        assert _selector_counts("frame", log) == {
            "FrameBody": 1, "Stream": 2, "Control": 3}
        assert _selector_counts("handle", root / "missing") == {
            "ResolverBoundary": None}
        assert _bounded_seconds(str(MAX_CAMPAIGN_SECONDS)) == MAX_CAMPAIGN_SECONDS
        try:
            _bounded_seconds(str(MAX_CAMPAIGN_SECONDS + 1))
        except argparse.ArgumentTypeError:
            pass
        else:
            raise AssertionError("overlong fuzz duration was accepted")
        deadline_log = root / "deadline.log"
        started = time.monotonic()
        assert _stream_command(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            root,
            deadline_log,
            os.environ.copy(),
            1,
        ) == 124
        assert time.monotonic() - started < 8
        assert "deadline exceeded" in deadline_log.read_text(encoding="utf-8")
    print("run_extended_fuzz_campaign.py self-test: PASS")


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def _bounded_seconds(raw: str) -> int:
    value = _positive_int(raw)
    if value > MAX_CAMPAIGN_SECONDS:
        raise argparse.ArgumentTypeError(
            f"duration must not exceed {MAX_CAMPAIGN_SECONDS} seconds"
        )
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--out", type=Path, default=Path("extended-fuzz-evidence"))
    parser.add_argument("--selector", choices=tuple(SELECTORS), default="frame")
    parser.add_argument("--sanitizer", choices=("asan", "ubsan"), default="asan")
    parser.add_argument("--seconds", type=_bounded_seconds, default=60)
    parser.add_argument("--seed", type=_positive_int, default=1)
    parser.add_argument("--bazel", default="bazel")
    parser.add_argument("--minimize", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--expected-commit")
    parser.add_argument("--expected-seed", type=_positive_int)
    parser.add_argument("--expected-selector", choices=tuple(SELECTORS))
    parser.add_argument("--expected-sanitizer", choices=("asan", "ubsan"))
    parser.add_argument("--expected-seconds", type=_bounded_seconds)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            _self_test()
            return 0
        if args.verify:
            output = args.out if args.out.is_absolute() else args.workspace / args.out
            _verify_evidence(
                output,
                args.expected_commit,
                args.expected_seed,
                args.expected_selector,
                args.expected_sanitizer,
                args.expected_seconds,
            )
            return 0
        return _run_campaign(args)
    except CampaignError as error:
        print(f"extended fuzz error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
