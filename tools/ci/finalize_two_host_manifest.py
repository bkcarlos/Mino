#!/usr/bin/env python3
"""Cross-check two physical-host role artifacts and write the final manifest."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.ci.two_host_protocol import finalize_evidence, self_test


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-manifest", type=Path)
    parser.add_argument("--client-manifest", type=Path)
    parser.add_argument("--out", type=Path, default=Path("two-host/final/manifest.json"))
    parser.add_argument("--expected-commit")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    missing = [
        name
        for name, value in (
            ("--server-manifest", args.server_manifest),
            ("--client-manifest", args.client_manifest),
        )
        if value is None
    ]
    if missing:
        parser.error("required arguments: " + ", ".join(missing))
    return finalize_evidence(
        server_manifest_path=args.server_manifest,
        client_manifest_path=args.client_manifest,
        output_path=args.out,
        expected_commit=args.expected_commit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
