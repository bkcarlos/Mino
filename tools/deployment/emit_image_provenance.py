#!/usr/bin/env python3
"""Emit bounded release provenance from BuildKit, image, and SPDX evidence."""

import argparse
import hashlib
import json
from pathlib import Path
import re


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--build-image", required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--source-dirty", choices=("true", "false"), default="false")
    parser.add_argument("--created", required=True)
    parser.add_argument("--iid-file", required=True, type=Path)
    parser.add_argument("--metadata-file", required=True, type=Path)
    parser.add_argument("--sbom-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    pinned_reference = re.compile(r"^.+@sha256:[0-9a-fA-F]{64}$")
    for name, reference in (
        ("build image", args.build_image),
        ("runtime image", args.runtime_image),
    ):
        if pinned_reference.fullmatch(reference) is None:
            raise SystemExit(f"{name} is not a complete sha256 digest reference")

    metadata = json.loads(args.metadata_file.read_text(encoding="utf-8"))
    iid = args.iid_file.read_text(encoding="utf-8").strip()
    image_digest = metadata.get("containerimage.digest", iid)
    if not isinstance(image_digest, str) or not image_digest.startswith("sha256:"):
        raise SystemExit("BuildKit did not emit a sha256 image digest/IID")
    document = {
        "schema_version": 1,
        "image_ref": args.image_ref,
        "image_digest": image_digest,
        "source_revision": args.revision,
        "source_dirty": args.source_dirty == "true",
        "created": args.created,
        "base_images": {
            "build": args.build_image,
            "runtime": args.runtime_image,
        },
        "sbom": {
            "format": "spdx-json",
            "path": args.sbom_file.name,
            "digest": sha256(args.sbom_file),
        },
        "buildkit_metadata": {
            "path": args.metadata_file.name,
            "digest": sha256(args.metadata_file),
        },
    }
    args.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
