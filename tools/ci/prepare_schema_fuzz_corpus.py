#!/usr/bin/env python3
"""Prepare deterministic selector-prefixed seeds for the schema fuzz target."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import tempfile


def _parse_hex(text: str) -> bytes:
    try:
        return bytes.fromhex(text)
    except ValueError as error:
        raise ValueError(f"invalid hexadecimal corpus input: {error}") from error


def _seed(selector: int, payload: bytes) -> bytes:
    if selector not in (0, 1, 2):
        raise ValueError(f"invalid fuzz selector: {selector}")
    return bytes((selector,)) + payload


def _write_seed(directory: Path, data: bytes) -> Path:
    digest = hashlib.sha256(data).hexdigest()
    destination = directory / digest
    if destination.is_symlink():
        raise RuntimeError(f"refusing symlink corpus entry: {destination}")
    if destination.exists():
        if not destination.is_file() or destination.read_bytes() != data:
            raise RuntimeError(f"corpus digest collision or invalid entry: {destination}")
        return destination

    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{digest}.", dir=directory, delete=False
        ) as temporary:
            temporary.write(data)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_name = temporary.name
        os.replace(temporary_name, destination)
        temporary_name = None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)
    return destination


def _prepare(workspace: Path, output: Path) -> dict[int, int]:
    sources = {
        0: (
            (workspace / "mino/schema/fuzz/testdata/canonical_payload.mino").read_bytes(),
            (workspace / "mino/schema/codegen/testdata/golden.mino").read_bytes(),
            b"",
        ),
        1: (
            (workspace / "mino/schema/codegen/testdata/golden.descriptor").read_bytes(),
            b"",
        ),
        2: (
            _parse_hex(
                (workspace / "mino/schema/fuzz/testdata/canonical_payload.wire.hex")
                .read_text(encoding="ascii")
            ),
            b"",
        ),
    }

    output.mkdir(parents=True, exist_ok=True)
    if output.is_symlink() or not output.is_dir():
        raise RuntimeError(f"corpus output is not a real directory: {output}")

    counts: dict[int, int] = {}
    for selector, payloads in sources.items():
        for payload in payloads:
            _write_seed(output, _seed(selector, payload))
        counts[selector] = len(payloads)
    return counts


def _self_test() -> None:
    assert _parse_hex("00 ff\n7F") == b"\x00\xff\x7f"
    assert _seed(0, b"idl") == b"\x00idl"
    assert _seed(1, b"") == b"\x01"
    assert _seed(2, b"wire") == b"\x02wire"
    try:
        _parse_hex("0xz")
    except ValueError:
        pass
    else:
        raise AssertionError("invalid hex input was accepted")

    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        first = _write_seed(directory, b"same")
        second = _write_seed(directory, b"same")
        assert first == second
        assert first.read_bytes() == b"same"
        assert len(list(directory.iterdir())) == 1
    print("prepare_schema_fuzz_corpus.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--out", type=Path, default=Path("fuzz-corpus"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return 0

    workspace = args.workspace.resolve()
    if not (workspace / "MODULE.bazel").is_file():
        parser.error(f"workspace does not contain MODULE.bazel: {workspace}")
    output = args.out
    if not output.is_absolute():
        output = workspace / output
    counts = _prepare(workspace, output)
    print(
        "prepared schema fuzz corpus: "
        + ", ".join(
            ("IDL", "Descriptor", "CanonicalPayload")[selector]
            + f"={counts[selector]}"
            for selector in sorted(counts)
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
