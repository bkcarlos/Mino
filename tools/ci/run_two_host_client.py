#!/usr/bin/env python3
"""Run the client role using the real Mino two-host probe binary."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.ci.two_host_protocol import (
    ProtocolError,
    load_token,
    resolve_commit,
    run_role,
    self_test,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("bazel-bin/tools/ci/mino_two_host_probe"))
    parser.add_argument("--server-address")
    parser.add_argument("--advertise-address")
    parser.add_argument("--port", type=int, default=43191)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--tcp-lane-count", type=int, default=1)
    parser.add_argument("--token-env", default="MINO_TWO_HOST_TOKEN")
    parser.add_argument("--commit")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--manifest", type=Path, default=Path("two-host/client/manifest.json"))
    parser.add_argument("--log", type=Path, default=Path("two-host/client/client.log"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    missing = [
        name
        for name, value in (
            ("--server-address", args.server_address),
            ("--advertise-address", args.advertise_address),
        )
        if not value
    ]
    if missing:
        parser.error("required arguments: " + ", ".join(missing))
    try:
        return run_role(
            role="client",
            binary=args.binary,
            address=args.server_address,
            advertise_address=args.advertise_address,
            port=args.port,
            timeout_seconds=args.timeout_seconds,
            tcp_lane_count=args.tcp_lane_count,
            token=load_token(os.environ, args.token_env),
            commit=resolve_commit(args.workspace, args.commit),
            manifest_path=args.manifest,
            log_path=args.log,
        )
    except ProtocolError as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
